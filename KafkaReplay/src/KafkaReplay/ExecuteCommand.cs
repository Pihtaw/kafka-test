using System.Text;
using Confluent.Kafka;

namespace KafkaReplay;

// --execute: отправить записи строго из plan.json, без повторного чтения источника и без повторной проверки.
// проверить план целиком -> отправить -> побайтово сверить отправленное -> статусы и отчeт
public static class ExecuteCommand
{
    public static async Task<int> RunAsync(CliOptions opt)
    {
        if (!PlanStore.TryLoad(opt.Plan!, out var plan, out var error)) return Cli.Fail(error!);

        // любое несовпадение контрольных сумм — отправка блокируется полностью
        var problems = plan!.Verify();
        if (problems.Count > 0)
            return Cli.Block($"план изменён или испорчен, ничего не отправлено:\n  - {string.Join("\n  - ", problems)}");

        // цель должна быть той же, что при построении плана
        if (!Connections.TryResolve(opt.TargetBootstrap, opt.TargetEnv, out var target, out error)) return Cli.Fail(error!);
        if (!Routes.SameCluster(plan.TargetBootstrap, target!.BootstrapServers))
            return Cli.Block($"цель {target.BootstrapServers} не совпадает с целью из плана {plan.TargetBootstrap}");

        if (Routes.SameCluster(plan.SourceBootstrap, plan.TargetBootstrap))
        {
            var back = plan.Items.Where(i => i.TargetTopic == plan.SourceTopic).Select(i => $"p{i.SourcePartition} o{i.SourceOffset}").ToList();
            if (back.Count > 0) return Cli.Block($"в плане есть отправка в топик-источник {plan.SourceTopic}: {string.Join(", ", back)}");
        }

        string statePath = ExecutionFile.PathFor(opt.Plan!);
        ExecutionFile? previous;
        try { previous = ExecutionFile.Load(statePath); }
        catch (System.Text.Json.JsonException e) { return Cli.Block($"{statePath} испорчен: {e.Message}"); }
        if (previous is not null && previous.PlanSha256 != plan.PlanSha256)
            return Cli.Block($"{statePath} относится к другому плану");

        var connection = target.ToClientConfig();
        var toSend = Executor.ToSend(plan, previous);
        Console.WriteLine($"План: {Path.GetFullPath(opt.Plan!)}");
        Console.WriteLine($"Цель: {target.Describe()}");
        Console.WriteLine($"Суммы плана сходятся ({plan.Items.Count} строк). К отправке в этот запуск: {toSend.Count}" +
                          (previous is null ? "" : $" (это повторный запуск: «записано» и «результат неизвестен» не повторяются)"));

        // отправка
        ExecutionFile state;
        try { state = await Executor.RunAsync(plan, previous, new KafkaPlanSender(connection), s => s.Save(statePath), DateTimeOffset.Now); }
        catch (KafkaException e) { return Cli.Fail($"нет связи с целью ({target.Describe()}): {e.Error.Reason}"); }

        // побайтовая сверка
        var verified = state.Verification.ToDictionary(v => v.Index);
        var toVerify = state.Items.Where(s => s.Status == DeliveryStatus.Written && s.Offset is not null && !(verified.TryGetValue(s.Index, out var v) && v.Matches)).ToList();
        if (toVerify.Count > 0)
        {
            using var fetcher = new KafkaRecordFetcher(connection);
            foreach (var s in toVerify)
            {
                var item = plan.Items[s.Index];
                verified[s.Index] = ByteCompare.Compare(item, fetcher.Fetch(item.TargetTopic, s.Partition!.Value, s.Offset!.Value));
            }
            state = state with { Verification = verified.Values.OrderBy(v => v.Index).ToList() };
            state.Save(statePath);
        }

        // не попавшие в план + строки плана
        var byStatus = Enum.GetValues<DeliveryStatus>().ToDictionary(s => s, _ => 0);
        foreach (var (status, count) in plan.NotInPlan)
            byStatus[Enum.GetValues<DeliveryStatus>().First(s => s.Ru() == status)] += count;
        foreach (var s in state.Items) byStatus[s.Status]++;
        int total = byStatus.Values.Sum();
        bool converges = total == plan.TotalRecords && state.Items.Count == plan.Items.Count;

        int written = state.Items.Count(s => s.Status == DeliveryStatus.Written);
        int matched = state.Verification.Count(v => v.Matches);
        int mismatched = state.Verification.Count(v => !v.Matches);

        Console.WriteLine();
        foreach (var s in state.Items)
        {
            var item = plan.Items[s.Index];
            var v = verified.GetValueOrDefault(s.Index);
            Console.WriteLine($"  p{item.SourcePartition} o{item.SourceOffset,-3} → {item.TargetTopic}/p{item.TargetPartition}" +
                              $"{(s.Offset is null ? "" : $" o{s.Offset}"),-6} {s.Status.Ru(),-20}" +
                              (v is null ? "" : v.Matches ? " байты совпали" : $" ❌ {string.Join("; ", v.Differences)}") +
                              (s.Error is null ? "" : $"  ({s.Error})"));
        }
        Console.WriteLine();
        foreach (var (status, count) in byStatus) Console.WriteLine($"  {status.Ru(),-21} {count}");
        Console.WriteLine($"Побайтовая сверка: совпало {matched} из {written} записанных" + (mismatched > 0 ? $", расхождений {mismatched}" : ""));
        Console.WriteLine(converges ? $"✅ Статусы сходятся: {total} из {plan.TotalRecords} записей" : $"❌ Статусы НЕ сходятся: {total} из {plan.TotalRecords}");

        string mdPath = await ReportMarkdown.WriteAsync(Path.GetDirectoryName(statePath)!, "execution.md", BuildMarkdown(plan, state, byStatus, target, converges));
        Console.WriteLine($"Статусы: {statePath}");
        Console.WriteLine($"         {mdPath}");

        bool allGood = converges && written == plan.Items.Count && mismatched == 0 && matched == written;
        int exitCode = !converges ? ExitCodes.Incomplete : allGood ? ExitCodes.Ok : ExitCodes.HasRejected;
        Console.WriteLine($"Код выхода {exitCode}: " + (exitCode switch
        {
            ExitCodes.Ok => "всё из плана записано и совпало байт в байт",
            ExitCodes.HasRejected => "есть «точно не записано», «результат неизвестен» или расхождения при сверке",
            _ => "статусы не сходятся"
        }));
        return exitCode;
    }

    private static string BuildMarkdown(PlanFile plan, ExecutionFile state, Dictionary<DeliveryStatus, int> byStatus, ConnectionSettings target, bool converges)
    {
        var verified = state.Verification.ToDictionary(v => v.Index);
        var md = new StringBuilder();
        md.AppendLine("# Отчёт KafkaReplay: отправка по плану").AppendLine();
        md.AppendLine($"- **Источник:** {plan.SourceDescription}, топик {plan.SourceTopic}");
        md.AppendLine($"- **Цель:** {target.Describe()}, партиционер {plan.Partitioner}");
        md.AppendLine($"- **План:** создан {plan.CreatedAt:yyyy-MM-dd HH:mm:ss zzz}, строк {plan.Items.Count}, сумма `{plan.PlanSha256[..16]}…` совпала").AppendLine();

        md.AppendLine("## Статусы всех прочитанных записей").AppendLine();
        md.AppendLine("| Статус | Записей |").AppendLine("|---|---|");
        foreach (var (status, count) in byStatus) md.AppendLine($"| {status.Ru()} | {count} |");
        md.AppendLine().AppendLine(converges ? $"✅ Сходится: {byStatus.Values.Sum()} из {plan.TotalRecords}" : $"❌ Не сходится: {byStatus.Values.Sum()} из {plan.TotalRecords}").AppendLine();

        md.AppendLine("## Строки плана").AppendLine();
        md.AppendLine("| Источник | Тип события | Цель | Статус | Сверка байтов |").AppendLine("|---|---|---|---|---|");
        foreach (var s in state.Items)
        {
            var item = plan.Items[s.Index];
            var v = verified.GetValueOrDefault(s.Index);
            string check = v is null ? "—" : v.Matches ? "совпало" : "❌ " + string.Join("; ", v.Differences);
            md.AppendLine($"| p{item.SourcePartition} o{item.SourceOffset} | {ReportMarkdown.ShortType(item.EventType)} | {item.TargetTopic} / p{item.TargetPartition}{(s.Offset is null ? "" : $" o{s.Offset}")} | {s.Status.Ru()} | {check} |");
        }
        return md.ToString();
    }
}
