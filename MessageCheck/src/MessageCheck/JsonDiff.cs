using System.Text.Json;

namespace MessageCheck;

public enum DifferenceKind
{
    Extra,          // поле есть в событии, контракт о нем не знает — сервис его пропустит
    MissingNull,    // поля нет в событии, сервис увидит null — обычно это новое необязательное поле
    MissingDefault, // поля нет в событии, сервис увидит 0 / "" / false — опасно: выглядит как настоящее значение
    Changed,         // поле есть, но после чтения значение другое (тип, округление, формат)
    Formatted
}

public record Difference(string Path, DifferenceKind Kind, string? Source, string? Result)
{
    public string Describe() => Kind switch
    {
        DifferenceKind.Extra => $"{Path}: лишнее поле, сервис его не увидит (было {Source})",
        DifferenceKind.MissingNull => $"{Path}: поля нет в событии, сервис увидит null",
        DifferenceKind.MissingDefault => $"{Path}: поля нет в событии, сервис молча увидит {Result}",
        DifferenceKind.Changed => $"{Path}: было {Source}, сервис увидит {Result}",
        DifferenceKind.Formatted => $"{Path}: то же значение в другой записи ({Source} → {Result})",
        _ => Path
    };

    // сервис прочитает, но данные не те, что отправили
    public bool IsWarning => Kind is DifferenceKind.MissingDefault or DifferenceKind.Changed;
}

public static class JsonDiff
{
    // порядок полей и пробелы не важны, 1.0 и 1 — одно и то же число
    public static IReadOnlyList<Difference> Compare(byte[] sourceJson, string resultJson)
    {
        using var source = JsonDocument.Parse(sourceJson);
        using var result = JsonDocument.Parse(resultJson);
        var diffs = new List<Difference>();
        Walk("$", source.RootElement, result.RootElement, diffs);
        return diffs;
    }

    public static string NoDifferencesReason(byte[] sourceJson)
    {
        using var doc = JsonDocument.Parse(sourceJson);
        int fields = doc.RootElement.ValueKind == JsonValueKind.Object ? doc.RootElement.EnumerateObject().Count() : 0;
        return $"отличий нет: все поля события ({fields}) прочитаны контрактом без изменений, новых полей в контракте нет";
    }

    private static void Walk(string path, JsonElement source, JsonElement result, List<Difference> diffs)
    {
        if (source.ValueKind == JsonValueKind.Object && result.ValueKind == JsonValueKind.Object)
        {
            var resultProps = result.EnumerateObject().ToDictionary(p => p.Name, p => p.Value, StringComparer.Ordinal);
            var sourceNames = new HashSet<string>(StringComparer.Ordinal);

            foreach (var p in source.EnumerateObject())
            {
                sourceNames.Add(p.Name);
                if (resultProps.TryGetValue(p.Name, out var r)) Walk($"{path}.{p.Name}", p.Value, r, diffs);
                else diffs.Add(new Difference($"{path}.{p.Name}", DifferenceKind.Extra, Short(p.Value), null));
            }
            foreach (var (name, r) in resultProps.Where(x => !sourceNames.Contains(x.Key)))
            {
                var kind = r.ValueKind == JsonValueKind.Null ? DifferenceKind.MissingNull : DifferenceKind.MissingDefault;
                diffs.Add(new Difference($"{path}.{name}", kind, null, Short(r)));
            }
            return;
        }

        if (source.ValueKind == JsonValueKind.Array && result.ValueKind == JsonValueKind.Array)
        {
            int a = source.GetArrayLength(), b = result.GetArrayLength();
            if (a != b) { diffs.Add(new Difference(path, DifferenceKind.Changed, $"{a} элементов", $"{b} элементов")); return; }
            for (int i = 0; i < a; i++) Walk($"{path}[{i}]", source[i], result[i], diffs);
            return;
        }

        if (SameValue(source, result)) return;
        var changeKind = SameUuid(source, result) ? DifferenceKind.Formatted : DifferenceKind.Changed;
        diffs.Add(new Difference(path, changeKind, Short(source), Short(result)));
    }

    // один и тот же UUID в другом регистре или формате
    private static bool SameUuid(JsonElement a, JsonElement b) =>
        a.ValueKind == JsonValueKind.String && b.ValueKind == JsonValueKind.String
        && Guid.TryParse(a.GetString(), out var x) && Guid.TryParse(b.GetString(), out var y)
        && x == y;

    private static bool SameValue(JsonElement a, JsonElement b)
    {
        if (a.ValueKind == JsonValueKind.Number && b.ValueKind == JsonValueKind.Number)
        {
            // 1.0 и 1 — одно число; если не влезает в decimal, сравниваем как double
            if (a.TryGetDecimal(out var x) && b.TryGetDecimal(out var y)) return x == y;
            return a.GetDouble().Equals(b.GetDouble());
        }
        if (a.ValueKind != b.ValueKind) return false;
        return a.ValueKind switch
        {
            JsonValueKind.String => string.Equals(a.GetString(), b.GetString(), StringComparison.Ordinal),
            JsonValueKind.True or JsonValueKind.False or JsonValueKind.Null => true,
            _ => string.Equals(a.GetRawText(), b.GetRawText()
, StringComparison.Ordinal)
        };
    }

    private static string Short(JsonElement e)
    {
        string text = e.GetRawText();
        return text.Length <= 60 ? text : text[..57] + "...";
    }
}
