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
        try { plan = await RangePlanner.PlanAsync(connection, opt.Topic!, fromMs, toMs).ConfigureAwait(false); }
        catch (KafkaException e) { return Cli.Fail($"нет связи с Kafka ({source.Describe()}): {e.Error.Reason}"); }
        if (plan is null) return ExitCodes.Blocked;

        Console.WriteLine($"Источник: {source.Describe()}");
        Console.WriteLine($"Контракты: {contracts?.Describe() ?? "встроенные учебные (MessageCheck.SampleContracts)"}");
        if (target is not null) Console.WriteLine($"Цель: {target.Describe()}, партиционер {opt.Partitioner.ToString().ToLowerInvariant()}");
        bool topicInRegistry = contracts?.HasTopic(opt.Topic!) ?? true;
        if (!topicInRegistry)
            Console.WriteLine($"  ⚠ топика {opt.Topic} нет в реестре контрактов: события читаются по Event-Type, но принадлежность топику не проверяется");
        Console.WriteLine($"Топик {opt.Topic}, время [{opt.From:yyyy-MM-dd HH:mm:ss} ; {opt.To:yyyy-MM-dd HH:mm:ss})");
        // хвост после конца окна: там ищем «опоздавших» — время внутри окна, а в логе после End
        var readTo = plan.ToDictionary(p => p.Partition, p => Window.ReadTo(p, opt.Tail));
        foreach (var p in plan)
            Console.WriteLine($"  партиция {p.Partition}: low={p.Low} high={p.High} → читаем [{p.Start}; {p.End}) = {p.End - p.Start} шт." +
                              (readTo[p.Partition] > p.End ? $", после окна просмотрим [{p.End}; {readTo[p.Partition]})" : ""));
        long expected = plan.Sum(p => readTo[p.Partition] - p.Start);

        // проверка каждой записи
        var processor = new Processor(catalog, new JsonModule(), TimeSpan.FromMilliseconds(500));
        var entries = new List<ReportEntry>();
        var items = new List<PlanItem>();
        var finished = new HashSet<int>();
        var tally = new ReadTally(plan, readTo);
        var tail = new TailStats();
        var endOf = plan.ToDictionary(p => p.Partition, p => p.End);

        foreach (var cr in RangePlanner.Read(connection, plan, finished, readTo))
        {
            tally.Add(cr.Partition.Value, cr.Offset.Value);   // считаем до проверки: независимо от отчёта

            long ts = cr.Message.Timestamp.UnixTimestampMs;
            var place = Window.Place(cr.Offset.Value, endOf[cr.Partition.Value], ts, fromMs, toMs);
            tail.Add(cr.Partition.Value, place);
            if (place == WindowPlace.AfterEnd) continue;   // хвост, время не наше — не проверяем

            // из Kafka в MessageCheck в байтах
            var record = new KafkaRecord(
                cr.Message.Headers?.Select(h => new Header(h.Key, h.GetValueBytes())).ToList() ?? [],
                cr.Message.Key,
                cr.Message.Value);

            var decision = await processor.ProcessAsync(record).ConfigureAwait(false);
            string? eventType = decision.Verdict == Verdict.Allowed ? EventTypeOf(record) : null;

            // тип прочитался, но по реестру он в другом топике — не ошибка
            if (eventType is not null && contracts is not null && topicInRegistry && !contracts.IsDeclaredFor(opt.Topic!, eventType))
                decision = decision with { Details = $"{decision.Details}; тип не объявлен в реестре для топика {opt.Topic}" };

            if (place == WindowPlace.LateAfterEnd)
                decision = decision with { Details = $"{decision.Details}; опоздавшая: время в окне, но в логе после конца окна — найдена в хвосте" };
            string? contractVersion = null;
            if (eventType is not null)
                contractVersion = contracts is null ? "учебный" : contracts.VersionOf(eventType);
            bool outsideWindow = place == WindowPlace.OutsideWindow;
            var entry = ReportEntry.From(cr, record, decision, outsideWindow, contractVersion);

            // в план попадают только разрешённые записи со временем в окне, для которых есть маршрут
            if (routes is not null && eventType is not null && outsideWindow)
                entry = entry with { Details = $"{entry.Details}; время вне окна — не отправляется" };
            else if (routes is not null && eventType is not null)
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
        var byVerdict = entries.GroupBy(e => e.Verdict, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var byReason = entries.GroupBy(e => e.Reason, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var byStatus = ReportSummary.ByStatus(entries);
        var rows = ReportSummary.Rows(entries);

        bool allRead = finished.Count == plan.Count;
        long gaps = expected - entries.Count - tail.TotalSkipped;
        bool sumMatches = tally.Matches(entries.Count, tail.TotalSkipped)   // прочитанное = записи отчёта + пропущенные в хвосте
                          && byVerdict.Values.Sum() == entries.Count  // у каждой записи ровно одно решение
                          && byStatus.Values.Sum() == entries.Count  // и ровно один статус
                          && rows.Sum(r => r.Count) == entries.Count;
        bool readOk = allRead && sumMatches;
        int rejected = entries.Count(e => e.IsRejected);
        int warnings = entries.Count(e => e.HasWarnings);
        int outside = entries.Count(e => e.OutsideWindow);
        bool tailComplete = plan.TrueForAll(p => readTo[p.Partition] == p.High);
        int exitCode = ExitCodes.For(allRead, sumMatches, rejected);

        // в консоль — только записи, на которые стоит посмотреть; полный список в report.md
        const int MaxConsoleRows = 20;
        var notable = entries.Where(e => !string.Equals(e.Verdict, nameof(MessageCheck.Verdict.Allowed), StringComparison.Ordinal) || e.HasWarnings || e.OutsideWindow).ToList();
        Console.WriteLine();
        string more = notable.Count > MaxConsoleRows ? $", первые {MaxConsoleRows}" : "";
        Console.WriteLine(notable.Count == 0
            ? "Требующих внимания записей нет (отказов, пропусков, ⚠ отличий, времени вне окна) — все записи в report.md"
            : $"Требуют внимания: {notable.Count} (отказы, пропуски, ⚠ отличия, время вне окна){more} — все записи в report.md");
        foreach (var e in notable.Take(MaxConsoleRows))
            Console.WriteLine($"  p{e.Partition} o{e.Offset,-3} {e.EventType ?? "—",-45} {e.Verdict,-15} {e.Reason,-22}" +
                              (e.TargetTopic is null ? "" : $" → {e.TargetTopic}/p{e.TargetPartition}") +
                              (e.HasWarnings ? "  ⚠ отличия" : "") +
                              (e.OutsideWindow ? "  ⚠ время вне окна" : ""));

        Console.WriteLine($"\nПрочитано {entries.Count} записей, offsets в диапазоне: {expected}" + (gaps > 0 ? $" (без записей: {gaps})" : ""));
        Console.WriteLine($"Дочитаны партиции: {finished.Count} из {plan.Count}");
        foreach (var p in plan)
            Console.WriteLine($"  партиция {p.Partition}: прочитано {tally.ReadIn(p.Partition)} из [{p.Start}; {readTo[p.Partition]})" +
                              (readTo[p.Partition] > p.End ? $" (окно [{p.Start}; {p.End}) + хвост: опоздавших {tail.Late(p.Partition)}, не наших {tail.Skipped(p.Partition)})" : "") +
                              (finished.Contains(p.Partition) ? "" : "  ⚠ не дочитана"));
        Console.WriteLine($"Сверка чтения: {tally.Describe(entries.Count, tail.TotalSkipped)}");
        Console.WriteLine(WindowSummary(tail.TotalLate, outside, tailComplete, opt.Tail));
        foreach (var (verdict, count) in byVerdict.OrderBy(x => x.Key, StringComparer.Ordinal)) Console.WriteLine($"  {verdict,-15} {count}");
        if (warnings > 0) Console.WriteLine($"  ⚠ записей с опасными отличиями: {warnings} (поле молча стало значением по умолчанию или изменилось)");
        Console.WriteLine(readOk ? "✅ Сводка сходится" : "❌ Сводка НЕ сходится");

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
            windowCheck = new { tail = opt.Tail == long.MaxValue ? "all" : opt.Tail.ToString(System.Globalization.CultureInfo.InvariantCulture), tailComplete, lateAfterEnd = tail.TotalLate, skippedInTail = tail.TotalSkipped, outsideWindow = outside },
            readCheck = new
            {
                consumed = tally.Consumed,
                duplicates = tally.Duplicates,
                outOfRange = tally.OutOfRange,
                perPartition = plan.Select(p => new
                {
                    p.Partition, read = tally.ReadIn(p.Partition), inRange = p.End - p.Start, readTo = readTo[p.Partition],
                    lateAfterEnd = tail.Late(p.Partition), skippedInTail = tail.Skipped(p.Partition), finished = finished.Contains(p.Partition)
                })
            },
            plan = routes is null ? null : new { items = items.Count, partitioner = opt.Partitioner.ToString().ToLowerInvariant(), routes = routes.All },
            exitCode,
            records = entries
        };
        string reportPath = await ReportWriter.WriteAsync(opt.OutDir, report).ConfigureAwait(false);
        string mdPath = await ReportMarkdown.WriteAsync(opt.OutDir, "report.md", ReportMarkdown.Build(
            "Отчёт KafkaReplay: проверка (ничего не отправлялось)",
            [("Источник", source.Describe()), ("Топик", opt.Topic!), ("Окно", $"[{opt.From:yyyy-MM-dd HH:mm:ss zzz} ; {opt.To:yyyy-MM-dd HH:mm:ss zzz})"),
             ("Контракты", contractsInfo), ("Цель", target?.Describe() ?? "нет (план не строился)"),
             ("Сверка чтения", $"{(readOk ? "✅" : "❌")} {tally.Describe(entries.Count, tail.TotalSkipped)}, дочитаны партиции {finished.Count} из {plan.Count}"),
             ("Окно по времени", WindowSummary(tail.TotalLate, outside, tailComplete, opt.Tail))],
            entries)).ConfigureAwait(false);
        Console.WriteLine($"Отчёт: {reportPath}");
        Console.WriteLine($"       {mdPath}");

        // план — только по полностью и правильно прочитанному окну: иначе --execute отправил бы неполные данные
        if (routes is not null && !readOk)
            Console.WriteLine("План отправки НЕ записан: окно прочитано не полностью или сверка чтения не сошлась — отправлять по нему нельзя");
        else if (routes is not null)
        {
            var notInPlan = entries.Where(e => e.TargetTopic is null).GroupBy(e => e.Status, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
            var planFile = PlanFile.Create(DateTimeOffset.Now, opt.Topic!, source.Describe(), source.BootstrapServers,
                target!.Describe(), target.BootstrapServers, opt.Partitioner.ToString().ToLowerInvariant(), contracts?.RegistryPath,
                entries.Count, notInPlan, items);
            string planPath = await PlanStore.SaveAsync(opt.OutDir, planFile).ConfigureAwait(false);
            Console.WriteLine($"План отправки: {planPath} — к отправке {items.Count} из {entries.Count}");
            Console.WriteLine($"Отправить: dotnet run -- --execute --plan {planPath} {(opt.TargetBootstrap is not null ? $"--target-bootstrap {opt.TargetBootstrap}" : $"--target-env {opt.TargetEnv}")}");
        }

        Console.WriteLine($"Код выхода {exitCode}: {ExitCodes.Describe(exitCode)}");
        return exitCode;
    }

    // одна строка про окно: опоздавшие найдены и проверены, чужие внутри не отправляются, хвост просмотрен до конца или нет
    private static string WindowSummary(long late, int outside, bool tailComplete, long tailLimit)
    {
        string lateText = late > 0
            ? $"⚠ опоздавших (время в окне, в логе после конца окна) найдено {late} — проверены и включены"
            : "опоздавших после конца окна нет";
        string outsideText = outside > 0 ? $"; ⚠ вне окна внутри диапазона {outside} — проверены, не отправляются" : "";
        string tailText;
        if (tailLimit == 0) tailText = "; хвост после окна не просматривался (--tail 0)";
        else if (tailComplete) tailText = "; хвост просмотрен до конца партиций";
        else tailText = $"; ⚠ хвост просмотрен только на {tailLimit} записей — дальше опоздавшие могли остаться (--tail all)";
        return lateText + outsideText + tailText;
    }

    // Event-Type уже проверен MessageCheck
    private static string? EventTypeOf(KafkaRecord r) =>
        r.Headers.Where(h => string.Equals(h.Key, HeaderNames.EventType, StringComparison.Ordinal) && h.Value is not null)
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
