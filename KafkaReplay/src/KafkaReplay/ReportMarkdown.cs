using System.Globalization;
using System.Text;
using MessageCheck;

namespace KafkaReplay;

// Наглядный отчёт report.md рядом с report.json: сводка, отличия по полям, таблица записей, подробности.
// Formatted (то же значение, записанное иначе, например UUID заглавными) в подробностях сворачивается в одну строку на запись:
// для сервиса это не отличие, а на реальных данных таких строк сотни на запись. Полный список всех отличий — в report.json.
public static class ReportMarkdown
{
    private const int MaxFieldsInLine = 8;

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
        foreach (var g in byVerdict) md.AppendLine(CultureInfo.InvariantCulture, $"| {g.Key} | {g.Count()} |");
        md.AppendLine().AppendLine("| Статус | Записей |").AppendLine("|---|---|");
        foreach (var (status, count) in byStatus) md.AppendLine(CultureInfo.InvariantCulture, $"| {status} | {count} |");
        md.AppendLine();
        md.AppendLine(converges
            ? $"✅ Сводки сходятся: записей {total}, по решениям {sumVerdict}, по статусам {sumStatus}, по строкам ниже {sumRows}."
            : $"❌ Сводки НЕ сходятся: записей {total}, по решениям {sumVerdict}, по статусам {sumStatus}, по строкам ниже {sumRows}.");
        md.AppendLine();

        md.AppendLine("## По типам событий, версиям и целевым топикам").AppendLine();
        md.AppendLine("| Тип события | Версия пакета | Целевой топик | Статус | Записей |").AppendLine("|---|---|---|---|---|");
        foreach (var r in rows) md.AppendLine(CultureInfo.InvariantCulture, $"| {Esc(r.EventType)} | {Esc(r.PackageVersion)} | {Esc(r.TargetTopic)} | {r.Status} | {r.Count} |");
        md.AppendLine();

        AppendDifferencesByField(md, entries);

        md.AppendLine("## Записи").AppendLine();
        md.AppendLine("| Партиция / offset | Тип события | Пакет → контракт | Решение | Причина | Статус | Цель | Отличия |").AppendLine("|---|---|---|---|---|---|---|---|");
        foreach (var e in entries)
        {
            string versions = $"{e.PackageVersion ?? "—"} → {e.ContractVersion ?? "—"}";
            string target = e.TargetTopic is null ? "—" : $"{e.TargetTopic} / p{e.TargetPartition}";
            md.AppendLine(CultureInfo.InvariantCulture, $"| p{e.Partition} o{e.Offset} | {Esc(ShortType(e.EventType))} | {Esc(versions)} | {e.Verdict} | {e.Reason} | {e.Status} | {Esc(target)} | {DiffCell(e)} |");
        }
        md.AppendLine();

        // подробности по каждой записи: исходник, JSON после чтения, отличия или почему их нет, решение
        md.AppendLine("## Подробно").AppendLine();
        foreach (var e in entries)
        {
            md.AppendLine(CultureInfo.InvariantCulture, $"### p{e.Partition} o{e.Offset} — {Esc(ShortType(e.EventType))}: {e.Verdict} / {e.Status}").AppendLine();
            md.AppendLine($"- **Решение:** {e.Verdict}, причина {e.Reason}{(e.Details is null ? "" : $" — {Esc(e.Details)}")}");
            md.AppendLine($"- **Исходное сообщение:** ключ `{Esc(e.Key ?? "null")}`, заголовки: {Esc(string.Join(", ", e.Headers.Select(h => $"{h.Key}={h.Value ?? "null"}")))}");
            md.AppendLine($"- **Тело:** `{Esc(Short(e.Payload ?? "null (tombstone)"))}`");
            md.AppendLine($"- **JSON после чтения контрактом:** {(e.ResultJson is null ? "нет" : $"`{Esc(Short(e.ResultJson))}`")}");
            AppendDifferences(md, e);
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

    // "$.Slots[2].Products[10].Id" → "$.Slots[].Products[].Id": одно и то же поле во всех элементах массивов
    public static string FieldPattern(string path)
    {
        var sb = new StringBuilder(path.Length);
        bool inIndex = false;
        foreach (char c in path)
        {
            if (c == '[') inIndex = true;
            else if (c == ']') inIndex = false;
            else if (inIndex && char.IsAsciiDigit(c)) continue;
            sb.Append(c);
        }
        return sb.ToString();
    }

    public static bool IsFormatted(ReportDifference d) =>
        string.Equals(d.Kind, nameof(DifferenceKind.Formatted), StringComparison.Ordinal);

    // ячейка «Отличия» в таблице записей: настоящие отличия отдельно от Formatted, например "1 ⚠ (+466 формат)"
    public static string DiffCell(ReportEntry e)
    {
        int formatted = e.Differences.Count(IsFormatted);
        int real = e.Differences.Count - formatted;
        string warn = e.HasWarnings ? " ⚠" : "";
        string main = real == 0 ? "нет" : real.ToString(CultureInfo.InvariantCulture) + warn;
        return formatted == 0 ? main : $"{main} (+{formatted.ToString(CultureInfo.InvariantCulture)} формат)";
    }

    // отличия одной записи: все настоящие — построчно, Formatted — одной строкой с полями и примером
    private static void AppendDifferences(StringBuilder md, ReportEntry e)
    {
        if (e.Differences.Count == 0)
        {
            md.AppendLine($"- **Отличия:** {Esc(e.DifferencesNote ?? "нет")}");
            return;
        }

        md.AppendLine("- **Отличия:**");
        foreach (var d in e.Differences.Where(d => !IsFormatted(d)))
            md.AppendLine($"  - {(d.Warning ? "⚠ " : "")}{Esc(d.Text)}");

        var formatted = e.Differences.Where(IsFormatted).ToList();
        if (formatted.Count > 0) md.AppendLine($"  - {Esc(FormattedLine(formatted))}");
    }

    public static string FormattedLine(IReadOnlyList<ReportDifference> formatted)
    {
        var fields = formatted
            .GroupBy(d => FieldPattern(d.Path), StringComparer.Ordinal)
            .Select(g => g.Count() == 1 ? g.Key : $"{g.Key} ×{g.Count().ToString(CultureInfo.InvariantCulture)}")
            .ToList();
        string shown = string.Join(", ", fields.Take(MaxFieldsInLine));
        string more = fields.Count > MaxFieldsInLine ? $" и ещё {(fields.Count - MaxFieldsInLine).ToString(CultureInfo.InvariantCulture)}" : "";
        return $"то же значение в другой записи, для сервиса не отличие — {formatted.Count.ToString(CultureInfo.InvariantCulture)} мест: {shown}{more}; например {formatted[0].Text}";
    }

    // сводная таблица: какое поле, каким видом отличия, в скольких записях. Видно сразу, без чтения подробностей
    private static void AppendDifferencesByField(StringBuilder md, IReadOnlyList<ReportEntry> entries)
    {
        var rows = entries
            .SelectMany(e => e.Differences
                .GroupBy(d => (Field: FieldPattern(d.Path), d.Kind, d.Warning))
                .Select(g => (Type: e.EventType ?? "—", g.Key.Field, g.Key.Kind, g.Key.Warning, Count: g.Count())))
            .GroupBy(x => (x.Type, x.Field, x.Kind, x.Warning))
            .Select(g => (g.Key.Type, g.Key.Field, g.Key.Kind, g.Key.Warning, Records: g.Count(), Total: g.Sum(x => x.Count)))
            .OrderByDescending(r => r.Warning)
            .ThenBy(r => r.Type, StringComparer.Ordinal)
            .ThenBy(r => r.Kind, StringComparer.Ordinal)
            .ThenBy(r => r.Field, StringComparer.Ordinal)
            .ToList();

        md.AppendLine("## Отличия по полям").AppendLine();
        if (rows.Count == 0)
        {
            md.AppendLine("Отличий нет.").AppendLine();
            return;
        }
        md.AppendLine("`[]` — поле во всех элементах массива. «Записей» — в скольких записях есть такое отличие, «Всего» — сколько раз оно встретилось. ⚠ — сервис увидит не те данные, что отправили.").AppendLine();
        md.AppendLine("| Тип события | Поле | Вид | Записей | Всего |").AppendLine("|---|---|---|---|---|");
        foreach (var r in rows)
            md.AppendLine(CultureInfo.InvariantCulture, $"| {Esc(r.Type)} | {Esc(r.Field)} | {(r.Warning ? "⚠ " : "")}{r.Kind} | {r.Records} | {r.Total} |");
        md.AppendLine();
    }

    private static string Short(string s) => s.Length <= 300 ? s : s[..297] + "...";

    // | ломает таблицы markdown
    private static string Esc(string s) => s.Replace("|", "\\|").Replace("\n", " ");
}
