using Confluent.Kafka;
using MessageCheck;
using Header = MessageCheck.Header;

namespace KafkaReplay;

// Без --execute: прочитать окно по времени -> проверить каждую запись -> отчeт -> (если есть маршруты) план отправки. Не пишем.
public static class DryRunCommand
{
    public static async Task<int> RunAsync(CliOptions opt)
    {
        if (!Connections.TryResolve(opt.Bootstrap, opt.SourceEnv, out var source, out var error)) return Cli.Fail(error!);
        var connection = source!.ToClientConfig();

        LoadedContracts? contracts = null;
        if (opt.Contracts is not null && !ContractsLoader.TryLoad(opt.Contracts, out contracts, out error)) return Cli.Fail(error!);
        var catalog = contracts?.Catalog ?? EventCatalog.Default();

        Routes? routes = null;
        ConnectionSettings? target = null;
        Dictionary<string, int> targetPartitions = [];
        if (opt.Routes is not null)
        {
            if (File.Exists(Path.Combine(opt.OutDir, "execution.json")))
                return Cli.Block($"в {Path.GetFullPath(opt.OutDir)} уже есть execution.json от прошлой отправки — укажи другую --out");

            if (!Routes.TryLoad(opt.Routes, out routes, out error)) return Cli.Fail(error!);
            if (!Connections.TryResolve(opt.TargetBootstrap, opt.TargetEnv, out target, out error)) return Cli.Fail(error!);

            // запрет тот же топик того же кластера, откуда читаем
            var back = routes!.RoutesBackToSource(opt.Topic!, source.BootstrapServers, target!.BootstrapServers);
            if (back.Count > 0)
                return Cli.Block($"маршрут ведёт в топик-источник {opt.Topic} того же кластера: {string.Join("; ", back)}");

            // целевые топики должны существовать
            try { targetPartitions = TargetTopics.Partitions(target.ToClientConfig(), routes.TargetTopics, out error); }
            catch (KafkaException e) { return Cli.Fail($"нет связи с целью ({target.Describe()}): {e.Error.Reason}"); }
            if (error is not null) return Cli.Block(error);
        }

        long fromMs = opt.From.ToUnixTimeMilliseconds(), toMs = opt.To.ToUnixTimeMilliseconds();

        // план чтения
        List<PartitionPlan>? plan;
        try { plan = await RangePlanner.PlanAsync(connection, opt.Topic!, fromMs, toMs); }
        catch (KafkaException e) { return Cli.Fail($"нет связи с Kafka ({source.Describe()}): {e.Error.Reason}"); }
        if (plan is null) return ExitCodes.Blocked;

        Console.WriteLine($"Источник: {source.Describe()}");
        Console.WriteLine($"Контракты: {contracts?.Describe() ?? "встроенные учебные (MessageCheck.SampleContracts)"}");
        if (target is not null) Console.WriteLine($"Цель: {target.Describe()}, партиционер {opt.Partitioner.ToString().ToLowerInvariant()}");
        bool topicInRegistry = contracts?.HasTopic(opt.Topic!) ?? true;
        if (!topicInRegistry)
            Console.WriteLine($"  ⚠ топика {opt.Topic} нет в реестре контрактов: события читаются по Event-Type, но принадлежность топику не проверяется");
        Console.WriteLine($"Топик {opt.Topic}, время [{opt.From:yyyy-MM-dd HH:mm:ss} ; {opt.To:yyyy-MM-dd HH:mm:ss})");
        foreach (var p in plan)
            Console.WriteLine($"  партиция {p.Partition}: low={p.Low} high={p.High} → читаем [{p.Start}; {p.End}) = {p.End - p.Start} шт.");
        long expected = plan.Sum(p => p.End - p.Start);

        // проверка каждой записи
        var processor = new Processor(catalog, new JsonModule(), TimeSpan.FromMilliseconds(500));
        var entries = new List<ReportEntry>();
        var items = new List<PlanItem>();
        var finished = new HashSet<int>();

        foreach (var cr in RangePlanner.Read(connection, plan, finished))
        {
            // из Kafka в MessageCheck в байтах
            var record = new KafkaRecord(
                cr.Message.Headers?.Select(h => new Header(h.Key, h.GetValueBytes())).ToList() ?? [],
                cr.Message.Key,
                cr.Message.Value);

            var decision = await processor.ProcessAsync(record);
            string? eventType = decision.Verdict == Verdict.Allowed ? EventTypeOf(record) : null;

            // тип прочитался, но по реестру он в другом топике — не ошибка
            if (eventType is not null && contracts is not null && topicInRegistry && !contracts.IsDeclaredFor(opt.Topic!, eventType))
                decision = decision with { Details = $"{decision.Details}; тип не объявлен в реестре для топика {opt.Topic}" };

            long ts = cr.Message.Timestamp.UnixTimestampMs;
            string? contractVersion = eventType is null ? null : contracts is null ? "учебный" : contracts.VersionOf(eventType);
            var entry = ReportEntry.From(cr, record, decision, outsideWindow: ts < fromMs || ts >= toMs, contractVersion);

            // в план попадают только разрешённые записи, для которых есть маршрут
            if (routes is not null && eventType is not null)
            {
                string? targetTopic = routes.TargetFor(eventType);
                if (targetTopic is null)
                    entry = entry with { Details = $"{entry.Details}; нет маршрута — не отправляется" };
                else
                {
                    int partition = Partitioning.Choose(opt.Partitioner, record.Key, cr.Partition.Value, targetPartitions[targetTopic]);
                    items.Add(PlanItem.Create(items.Count, cr.Partition.Value, cr.Offset.Value, ts, eventType, record, targetTopic, partition));
                    entry = entry.WithStatus(DeliveryStatus.NotSent, targetTopic, partition);
                }
            }
            entries.Add(entry);
        }

        // сводка
        var byVerdict = entries.GroupBy(e => e.Verdict).ToDictionary(g => g.Key, g => g.Count());
        var byReason = entries.GroupBy(e => e.Reason).ToDictionary(g => g.Key, g => g.Count());
        var byStatus = ReportSummary.ByStatus(entries);
        var rows = ReportSummary.Rows(entries);

        bool allRead = finished.Count == plan.Count;
        long gaps = expected - entries.Count;
        bool sumMatches = byVerdict.Values.Sum() == entries.Count  // у каждой записи ровно одно решение
                          && byStatus.Values.Sum() == entries.Count  // и ровно один статус
                          && rows.Sum(r => r.Count) == entries.Count;
        int rejected = entries.Count(e => e.IsRejected);
        int warnings = entries.Count(e => e.HasWarnings);
        int exitCode = ExitCodes.For(allRead, sumMatches, rejected);

        Console.WriteLine();
        foreach (var e in entries)
            Console.WriteLine($"  p{e.Partition} o{e.Offset,-3} {e.EventType ?? "—",-45} {e.Verdict,-15} {e.Reason,-22}" +
                              (e.TargetTopic is null ? "" : $" → {e.TargetTopic}/p{e.TargetPartition}") +
                              (e.HasWarnings ? "  ⚠ отличия" : "") +
                              (e.OutsideWindow ? "  ⚠ время вне окна" : ""));

        Console.WriteLine($"\nПрочитано {entries.Count} записей, offsets в диапазоне: {expected}" + (gaps > 0 ? $" (без записей: {gaps})" : ""));
        Console.WriteLine($"Дочитаны партиции: {finished.Count} из {plan.Count}");
        foreach (var (verdict, count) in byVerdict.OrderBy(x => x.Key)) Console.WriteLine($"  {verdict,-15} {count}");
        if (warnings > 0) Console.WriteLine($"  ⚠ записей с опасными отличиями: {warnings} (поле молча стало значением по умолчанию или изменилось)");
        Console.WriteLine(allRead && sumMatches ? "✅ Сводка сходится" : "❌ Сводка НЕ сходится");

        // отчет
        var contractsInfo = contracts?.Describe() ?? "встроенные учебные (MessageCheck.SampleContracts)";
        var report = new
        {
            mode = "dry-run (ничего не отправлялось)",
            source = source.Describe(),
            target = target?.Describe(),
            contracts = new { path = contracts?.RegistryPath, version = contracts?.RegistryVersion, description = contractsInfo },
            topicInRegistry,
            topic = opt.Topic,
            window = new { from = opt.From, to = opt.To, fromMs, toMs },
            partitions = plan,
            summary = new { read = entries.Count, offsetsInRange = expected, gaps, partitionsFinished = finished.Count, allRead, sumMatches, rejected, warnings, byVerdict, byReason, byStatus, rows },
            plan = routes is null ? null : new { items = items.Count, partitioner = opt.Partitioner.ToString().ToLowerInvariant(), routes = routes.All },
            exitCode,
            records = entries
        };
        string reportPath = await ReportWriter.WriteAsync(opt.OutDir, report);
        string mdPath = await ReportMarkdown.WriteAsync(opt.OutDir, "report.md", ReportMarkdown.Build(
            "Отчёт KafkaReplay: проверка (ничего не отправлялось)",
            [("Источник", source.Describe()), ("Топик", opt.Topic!), ("Окно", $"[{opt.From:yyyy-MM-dd HH:mm:ss zzz} ; {opt.To:yyyy-MM-dd HH:mm:ss zzz})"),
             ("Контракты", contractsInfo), ("Цель", target?.Describe() ?? "нет (план не строился)")],
            entries));
        Console.WriteLine($"Отчёт: {reportPath}");
        Console.WriteLine($"       {mdPath}");

        if (routes is not null)
        {
            var notInPlan = entries.Where(e => e.TargetTopic is null).GroupBy(e => e.Status).ToDictionary(g => g.Key, g => g.Count());
            var planFile = PlanFile.Create(DateTimeOffset.Now, opt.Topic!, source.Describe(), source.BootstrapServers,
                target!.Describe(), target.BootstrapServers, opt.Partitioner.ToString().ToLowerInvariant(), contracts?.RegistryPath,
                entries.Count, notInPlan, items);
            string planPath = await PlanStore.SaveAsync(opt.OutDir, planFile);
            Console.WriteLine($"План отправки: {planPath} — к отправке {items.Count} из {entries.Count}");
            Console.WriteLine($"Отправить: dotnet run -- --execute --plan {planPath} {(opt.TargetBootstrap is not null ? $"--target-bootstrap {opt.TargetBootstrap}" : $"--target-env {opt.TargetEnv}")}");
        }

        Console.WriteLine($"Код выхода {exitCode}: {ExitCodes.Describe(exitCode)}");
        return exitCode;
    }

    // Event-Type уже проверен MessageCheck
    private static string? EventTypeOf(KafkaRecord r) =>
        r.Headers.Where(h => h.Key == HeaderNames.EventType && h.Value is not null)
                 .Select(h => System.Text.Encoding.UTF8.GetString(h.Value!)).FirstOrDefault();
}

public static class TargetTopics
{
    // число партиций каждого целевого топика
    public static Dictionary<string, int> Partitions(ClientConfig target, IEnumerable<string> topics, out string? error)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig(target)).Build();
        var result = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var topic in topics)
        {
            var meta = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics[0];
            if (meta.Error.IsError || meta.Partitions.Count == 0)
            {
                error = $"целевого топика {topic} нет в цели ({meta.Error.Reason}). Создай его заранее: утилита топики не создаёт";
                return result;
            }
            result[topic] = meta.Partitions.Count;
        }
        error = null;
        return result;
    }
}
