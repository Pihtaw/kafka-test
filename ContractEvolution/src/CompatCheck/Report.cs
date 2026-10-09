using System.Text;
using Contracts;

namespace CompatCheck;

public static class Report
{
    public static string Icon(Outcome o) => o switch
    {
        Outcome.Ok => "ок",
        Outcome.ExtraIgnored => "новые поля не видим",
        Outcome.SilentDefault or Outcome.ValueChanged => "данные не те",
        _ => "не ок"
    };

    public const string Legend =
        "все события прочитались без потерь · " +
        "прочитались, новые поля старый контракт просто не видит · " +
        "прочитались БЕЗ ошибки, но данные не те: поле не пришло и стало default или значение изменилось · " +
        "десериализация упала. Число — сколько событий из скольких с этим исходом.";

    // ячейка отчета: худший исход среди событий пары и сколько событий с ним
    public static string Cell(IEnumerable<CheckResult> pair)
    {
        var list = pair.ToList();
        var worst = list.Max(r => r.Outcome);
        int bad = list.Count(r => r.Outcome == worst);
        return worst == Outcome.Ok ? "✅" : $"{Icon(worst)} {bad}/{list.Count}";
    }

    public static string Matrix(List<CheckResult> results)
    {
        var versions = OrderCreatedVersions.All.Select(v => v.Version).ToList();
        var sb = new StringBuilder();
        sb.AppendLine("| отправил ↓ / читает → | " + string.Join(" | ", versions.Select(v => $"v{v}")) + " |");
        sb.AppendLine("|---|" + string.Concat(versions.Select(_ => "---|")));
        foreach (var p in versions)
        {
            var cells = versions.Select(c => c > p ? "" : Cell(results.Where(r => r.Producer == p && r.Consumer == c)));
            sb.AppendLine($"| **v{p}** | " + string.Join(" | ", cells) + " |");
        }
        return sb.ToString();
    }

    public static string Markdown(IEnumerable<(ConsumerMode Mode, List<CheckResult> Results)> runs, string source)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Совместимость версий OrderCreated");
        sb.AppendLine();
        sb.AppendLine($"Источник событий: {source}. Вопрос: может ли сервис со СТАРОЙ версией контракта прочитать событие, отправленное НОВОЙ версией.");
        sb.AppendLine();
        sb.AppendLine("## Версии");
        sb.AppendLine();
        sb.AppendLine("| версия | что поменялось |");
        sb.AppendLine("|---|---|");
        foreach (var v in OrderCreatedVersions.All) sb.AppendLine($"| v{v.Version} | {v.Change} |");
        sb.AppendLine();

        foreach (var (mode, results) in runs)
        {
            sb.AppendLine($"## Потребитель: {mode.Name}");
            sb.AppendLine();
            sb.AppendLine(Matrix(results));
            sb.AppendLine(Legend);
            sb.AppendLine();
            sb.AppendLine("### Что пошло не так");
            sb.AppendLine();
            sb.AppendLine("| отправил → читает | исход | подробности | на каких событиях |");
            sb.AppendLine("|---|---|---|---|");
            var groups = results.Where(r => r.Outcome is not (Outcome.Ok or Outcome.ExtraIgnored))
                .GroupBy(r => (r.Producer, r.Consumer, r.Outcome, r.Details))
                .OrderBy(g => g.Key.Producer).ThenBy(g => g.Key.Consumer).ThenByDescending(g => g.Key.Outcome);
            foreach (var g in groups)
            {
                int total = results.Count(r => r.Producer == g.Key.Producer && r.Consumer == g.Key.Consumer);
                string cases = g.Count() == total ? $"все {total}" : string.Join(", ", g.Select(r => r.Case));
                sb.AppendLine($"| v{g.Key.Producer} → v{g.Key.Consumer} | {Icon(g.Key.Outcome)} {g.Key.Outcome} | {g.Key.Details.Replace("|", "\\|")} | {cases} |");
            }
            sb.AppendLine();
        }
        return sb.ToString();
    }
}
