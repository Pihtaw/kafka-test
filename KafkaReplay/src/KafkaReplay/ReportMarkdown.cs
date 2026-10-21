using System.Text;

namespace KafkaReplay;

// Наглядный отчёт report.md рядом с report.json: сводка, таблица записей, отличия
public static class ReportMarkdown
{
    public static string Build(string title, IReadOnlyList<(string Name, string Value)> info, IReadOnlyList<ReportEntry> entries)
    {
        var md = new StringBuilder();
        md.AppendLine($"# {title}").AppendLine();
        foreach (var (name, value) in info) md.AppendLine($"- **{name}:** {Esc(value)}");
        md.AppendLine();

        // сводки
        var byVerdict = entries.GroupBy(e => e.Verdict, StringComparer.Ordinal).OrderBy(g => g.Key, StringComparer.Ordinal).ToList();
        var byStatus = ReportSummary.ByStatus(entries);
        var rows = ReportSummary.Rows(entries);
        int total = entries.Count, sumVerdict = byVerdict.Sum(g => g.Count()), sumStatus = byStatus.Values.Sum(), sumRows = rows.Sum(r => r.Count);
        bool converges = sumVerdict == total && sumStatus == total && sumRows == total;

        md.AppendLine("## Сводка").AppendLine();
        md.AppendLine("| Решение | Записей |").AppendLine("|---|---|");
        foreach (var g in byVerdict) md.AppendLine(System.Globalization.CultureInfo.InvariantCulture, $"| {g.Key} | {g.Count()} |");
        md.AppendLine().AppendLine("| Статус | Записей |").AppendLine("|---|---|");
        foreach (var (status, count) in byStatus) md.AppendLine(System.Globalization.CultureInfo.InvariantCulture, $"| {status} | {count} |");
        md.AppendLine();
        md.AppendLine(converges
            ? $"✅ Сводки сходятся: записей {total}, по решениям {sumVerdict}, по статусам {sumStatus}, по строкам ниже {sumRows}."
            : $"❌ Сводки НЕ сходятся: записей {total}, по решениям {sumVerdict}, по статусам {sumStatus}, по строкам ниже {sumRows}.");
        md.AppendLine();

        md.AppendLine("## По типам событий, версиям и целевым топикам").AppendLine();
        md.AppendLine("| Тип события | Версия пакета | Целевой топик | Статус | Записей |").AppendLine("|---|---|---|---|---|");
        foreach (var r in rows) md.AppendLine(System.Globalization.CultureInfo.InvariantCulture, $"| {Esc(r.EventType)} | {Esc(r.PackageVersion)} | {Esc(r.TargetTopic)} | {r.Status} | {r.Count} |");
        md.AppendLine();

        md.AppendLine("## Записи").AppendLine();
        md.AppendLine("| Партиция / offset | Тип события | Пакет → контракт | Решение | Причина | Статус | Цель | Отличия |").AppendLine("|---|---|---|---|---|---|---|---|");
        foreach (var e in entries)
        {
            string versions = $"{e.PackageVersion ?? "—"} → {e.ContractVersion ?? "—"}";
            string target = e.TargetTopic is null ? "—" : $"{e.TargetTopic} / p{e.TargetPartition}";
            string warn = e.HasWarnings ? " ⚠" : "";
            string diffs = e.Differences.Count == 0 ? "нет" : $"{e.Differences.Count}{warn}";
            md.AppendLine(System.Globalization.CultureInfo.InvariantCulture, $"| p{e.Partition} o{e.Offset} | {Esc(ShortType(e.EventType))} | {Esc(versions)} | {e.Verdict} | {e.Reason} | {e.Status} | {Esc(target)} | {diffs} |");
        }
        md.AppendLine();

        // подробности по каждой записи: исходник, JSON после чтения, отличия или почему их нет, решение
        md.AppendLine("## Подробно").AppendLine();
        foreach (var e in entries)
        {
            md.AppendLine(System.Globalization.CultureInfo.InvariantCulture, $"### p{e.Partition} o{e.Offset} — {Esc(ShortType(e.EventType))}: {e.Verdict} / {e.Status}").AppendLine();
            md.AppendLine($"- **Решение:** {e.Verdict}, причина {e.Reason}{(e.Details is null ? "" : $" — {Esc(e.Details)}")}");
            md.AppendLine($"- **Исходное сообщение:** ключ `{Esc(e.Key ?? "null")}`, заголовки: {Esc(string.Join(", ", e.Headers.Select(h => $"{h.Key}={h.Value ?? "null"}")))}");
            md.AppendLine($"- **Тело:** `{Esc(Short(e.Payload ?? "null (tombstone)"))}`");
            md.AppendLine($"- **JSON после чтения контрактом:** {(e.ResultJson is null ? "нет" : $"`{Esc(Short(e.ResultJson))}`")}");
            if (e.Differences.Count == 0) md.AppendLine($"- **Отличия:** {Esc(e.DifferencesNote ?? "нет")}");
            else
            {
                md.AppendLine("- **Отличия:**");
                foreach (var d in e.Differences) md.AppendLine($"  - {(d.Warning ? "⚠ " : "")}{Esc(d.Text)}");
            }
            md.AppendLine();
        }
        return md.ToString();
    }

    public static async Task<string> WriteAsync(string outDir, string fileName, string content)
    {
        Directory.CreateDirectory(outDir);
        string path = Path.Combine(outDir, fileName);
        await File.WriteAllTextAsync(path, content).ConfigureAwait(false);
        return Path.GetFullPath(path);
    }

    // "MessageCheck.SampleContracts.OrderCreated" → "OrderCreated"
    public static string ShortType(string? fullName) => fullName is null ? "—" : fullName[(fullName.LastIndexOf('.') + 1)..];

    private static string Short(string s) => s.Length <= 300 ? s : s[..297] + "...";

    // | ломает таблицы markdown
    private static string Esc(string s) => s.Replace("|", "\\|").Replace("\n", " ");
}
