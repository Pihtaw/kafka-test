using System.Globalization;
using System.Text.Json;

namespace MessageCheck;

// нарушение JSON-схемы: где, какое правило схемы, что не так
public record SchemaViolation(string Path, string Rule, string Message)
{
    public string Describe() => $"{Path}: схема ({Rule}): {Message}";

    // Без ⚠ — то, что C#-контракт читает так же, как правильное событие:
    //   format        — формат строки (например, время без часового пояса);
    //   required-null — нет поля, которое может быть null: для контракта это то же самое, что null.
    // ⚠ — событие не совпадает со схемой по сути: null там, где нельзя, нет обязательного поля, не тот тип, лишнее поле
    public bool IsWarning => Rule is not ("format" or "required-null");
}

// Проверка сырого тела события по JSON-схеме контракта — до чтения C#-классом.
// Ловит то, что десериализатор пропускает молча: обязательного поля нет, null там, где нельзя,
// значение не из списка (enum), не тот тип, поле, которого нет в схеме.
// Поддерживаем то, что встречается в схемах контрактов: type (в т.ч. ["string","null"]), required, properties,
// additionalProperties, items, enum, format (uuid, date-time, int32, int64), $ref на #/definitions или #/$defs, allOf/anyOf/oneOf.
// Остальные ключевые слова не проверяются, и это не тихо: Unsupported перечисляет их, SchemaSet показывает при загрузке
public static class SchemaValidator
{
    // на одну запись больше не собираем: в отчёте хватит, а массив из тысяч элементов не раздует его
    public const int MaxViolations = 100;

    private static readonly HashSet<string> Checked = new(StringComparer.Ordinal)
        { "type", "properties", "required", "additionalProperties", "items", "enum", "format", "$ref", "allOf", "anyOf", "oneOf" };

    // описания: на проверку не влияют
    private static readonly HashSet<string> Annotations = new(StringComparer.Ordinal)
        { "$schema", "$id", "id", "title", "description", "definitions", "$defs", "default", "examples", "deprecated", "readOnly", "writeOnly", "$comment" };

    public static IReadOnlyList<SchemaViolation> Validate(JsonElement schema, byte[] payload)
    {
        var found = new List<SchemaViolation>();
        try
        {
            using var doc = JsonDocument.Parse(payload);
            Check(new Context(schema, found), schema, doc.RootElement, "$");
        }
        catch (JsonException e)
        {
            found.Add(new SchemaViolation("$", "json", $"тело не JSON: {e.Message}"));
        }
        return found;
    }

    // ключевые слова схемы, которые валидатор не проверяет
    public static IReadOnlySet<string> Unsupported(JsonElement schema)
    {
        var found = new SortedSet<string>(StringComparer.Ordinal);
        Collect(schema, found);
        return found;
    }

    private sealed class Context(JsonElement root, List<SchemaViolation> found)
    {
        public JsonElement Root { get; } = root;
        public bool Full => found.Count >= MaxViolations;

        public void Add(string path, string rule, string message)
        {
            if (!Full) found.Add(new SchemaViolation(path, rule, message));
        }
    }

    private static void Check(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        if (ctx.Full) return;
        if (schema.ValueKind == JsonValueKind.False)
        {
            ctx.Add(path, "false", "схема запрещает здесь любое значение");
            return;
        }
        if (schema.ValueKind != JsonValueKind.Object) return;   // true — ограничений нет

        if (schema.TryGetProperty("$ref", out var reference))
        {
            CheckRef(ctx, reference, value, path);
            return;   // в draft-04 ключи рядом с $ref не действуют
        }
        if (!CheckType(ctx, schema, value, path)) return;   // тип не тот — остальное проверять бессмысленно

        CheckEnum(ctx, schema, value, path);
        CheckFormat(ctx, schema, value, path);
        CheckCombinators(ctx, schema, value, path);
        if (value.ValueKind == JsonValueKind.Object) CheckObject(ctx, schema, value, path);
        else if (value.ValueKind == JsonValueKind.Array) CheckArray(ctx, schema, value, path);
    }

    private static void CheckRef(Context ctx, JsonElement reference, JsonElement value, string path)
    {
        string? link = reference.ValueKind == JsonValueKind.String ? reference.GetString() : null;
        if (TryResolve(ctx.Root, link, out var target)) Check(ctx, target, value, path);
        else ctx.Add(path, "$ref", $"ссылка {link} не найдена в схеме");
    }

    // только ссылки внутри файла: #/definitions/Имя, #/$defs/Имя
    private static bool TryResolve(JsonElement root, string? link, out JsonElement target)
    {
        target = root;
        if (link is null || !link.StartsWith('#')) return false;
        var parts = link[1..].Split('/', StringSplitOptions.RemoveEmptyEntries);
        for (int i = 0; i < parts.Length; i++)
        {
            string name = parts[i].Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
            if (target.ValueKind != JsonValueKind.Object || !target.TryGetProperty(name, out target)) return false;
        }
        return true;
    }

    // type: строка или массив строк, например ["string", "null"]
    private static bool CheckType(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        if (!schema.TryGetProperty("type", out var type)) return true;
        List<string> allowed = type.ValueKind == JsonValueKind.Array
            ? type.EnumerateArray().Select(t => t.GetString() ?? "").ToList()
            : [type.GetString() ?? ""];
        if (allowed.Exists(t => IsOfType(value, t))) return true;

        ctx.Add(path, "type", $"ожидали {string.Join(" или ", allowed)}, пришло {Describe(value)}");
        return false;
    }

    private static bool IsOfType(JsonElement value, string type) => type switch
    {
        "null" => value.ValueKind == JsonValueKind.Null,
        "boolean" => value.ValueKind is JsonValueKind.True or JsonValueKind.False,
        "object" => value.ValueKind == JsonValueKind.Object,
        "array" => value.ValueKind == JsonValueKind.Array,
        "string" => value.ValueKind == JsonValueKind.String,
        "number" => value.ValueKind == JsonValueKind.Number,
        "integer" => value.ValueKind == JsonValueKind.Number && IsWhole(value),
        _ => true   // незнакомый тип не ограничиваем
    };

    // схема поля допускает null: "type": "null" или ["…", "null"]
    private static bool AllowsNull(JsonElement schema) =>
        schema.ValueKind == JsonValueKind.Object && schema.TryGetProperty("type", out var type) &&
        (type.ValueKind == JsonValueKind.Array
            ? type.EnumerateArray().Any(t => string.Equals(t.GetString(), "null", StringComparison.Ordinal))
            : string.Equals(type.GetString(), "null", StringComparison.Ordinal));

    private static bool IsWhole(JsonElement number) =>
        number.TryGetInt64(out _) || (number.TryGetDecimal(out var d) && d == decimal.Truncate(d));

    // enum: значение должно совпасть с одним из списка; регистр важен — "Kitchen" не "kitchen"
    private static void CheckEnum(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        if (!schema.TryGetProperty("enum", out var options) || options.ValueKind != JsonValueKind.Array) return;
        if (options.EnumerateArray().Any(o => JsonElement.DeepEquals(o, value))) return;

        string list = string.Join(", ", options.EnumerateArray().Select(o => o.GetRawText()));
        ctx.Add(path, "enum", $"значение {Short(value.GetRawText())} не из списка: {list}");
    }

    private static void CheckFormat(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        if (!schema.TryGetProperty("format", out var f) || f.ValueKind != JsonValueKind.String) return;
        string format = f.GetString()!;
        string? problem = format switch
        {
            "uuid" or "guid" when value.ValueKind == JsonValueKind.String => UuidProblem(value.GetString()!),
            "date-time" when value.ValueKind == JsonValueKind.String => DateTimeProblem(value.GetString()!),
            "int32" when value.ValueKind == JsonValueKind.Number => value.TryGetInt32(out _) ? null : "не помещается в int32",
            "int64" when value.ValueKind == JsonValueKind.Number => value.TryGetInt64(out _) ? null : "не помещается в int64",
            _ => null
        };
        if (problem is not null) ctx.Add(path, "format", $"{format}: {problem} ({Short(value.GetRawText())})");
    }

    // Uuid в событиях Dodo — 32 hex-символа без дефисов; вид с дефисами тоже принимаем
    private static string? UuidProblem(string s) =>
        Guid.TryParseExact(s, "N", out _) || Guid.TryParseExact(s, "D", out _) ? null : "не UUID";

    // date-time по RFC 3339 — с часовым поясом: Z или +03:00
    private static string? DateTimeProblem(string s)
    {
        if (!DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var t)) return "не дата-время";
        return t.Kind == DateTimeKind.Unspecified ? "нет часового пояса (Z или +hh:mm), а date-time его требует" : null;
    }

    private static void CheckCombinators(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        if (schema.TryGetProperty("allOf", out var all) && all.ValueKind == JsonValueKind.Array)
        {
            foreach (var s in all.EnumerateArray()) Check(ctx, s, value, path);
        }
        if (schema.TryGetProperty("anyOf", out var any) && any.ValueKind == JsonValueKind.Array && Matches(ctx.Root, any, value) == 0)
            ctx.Add(path, "anyOf", "не подходит ни один вариант anyOf");
        if (schema.TryGetProperty("oneOf", out var one) && one.ValueKind == JsonValueKind.Array)
        {
            int n = Matches(ctx.Root, one, value);
            if (n == 0) ctx.Add(path, "oneOf", "не подходит ни один вариант oneOf");
            else if (n > 1) ctx.Add(path, "oneOf", $"подходят сразу {n.ToString(CultureInfo.InvariantCulture)} варианта oneOf, а должен один");
        }
    }

    // сколько вариантов принимают значение; ошибки отдельных вариантов наружу не идут
    private static int Matches(JsonElement root, JsonElement variants, JsonElement value) =>
        variants.EnumerateArray().Count(v =>
        {
            var probe = new List<SchemaViolation>();
            Check(new Context(root, probe), v, value, "$");
            return probe.Count == 0;
        });

    private static void CheckObject(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        var present = value.EnumerateObject().Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        bool hasProperties = schema.TryGetProperty("properties", out var properties) && properties.ValueKind == JsonValueKind.Object;
        if (schema.TryGetProperty("required", out var required) && required.ValueKind == JsonValueKind.Array)
        {
            foreach (var name in required.EnumerateArray().Select(r => r.GetString() ?? "").Where(n => !present.Contains(n)))
            {
                // генераторы схем из контрактов перечисляют в required все поля, в том числе nullable
                bool nullable = hasProperties && properties.TryGetProperty(name, out var fieldSchema) && AllowsNull(fieldSchema);
                if (nullable) ctx.Add($"{path}.{name}", "required-null", "поля нет; схема требует его, но допускает null — контракт прочитает как null");
                else ctx.Add($"{path}.{name}", "required", "обязательного поля нет в событии");
            }
        }

        bool hasAdditional = schema.TryGetProperty("additionalProperties", out var additional);
        foreach (var p in value.EnumerateObject())
        {
            string child = $"{path}.{p.Name}";
            if (hasProperties && properties.TryGetProperty(p.Name, out var propertySchema)) Check(ctx, propertySchema, p.Value, child);
            else if (hasAdditional && additional.ValueKind == JsonValueKind.False) ctx.Add(child, "additionalProperties", "поля нет в схеме, а лишние поля схема запрещает");
            else if (hasAdditional && additional.ValueKind == JsonValueKind.Object) Check(ctx, additional, p.Value, child);
        }
    }

    // items — одна схема на все элементы; items-массив (кортеж) не поддерживаем
    private static void CheckArray(Context ctx, JsonElement schema, JsonElement value, string path)
    {
        if (!schema.TryGetProperty("items", out var items) || items.ValueKind == JsonValueKind.Array) return;
        foreach (var (item, i) in value.EnumerateArray().Select((x, i) => (x, i)))
            Check(ctx, items, item, $"{path}[{i.ToString(CultureInfo.InvariantCulture)}]");
    }

    private static string Describe(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => "null",
        JsonValueKind.True or JsonValueKind.False => $"boolean {value.GetRawText()}",
        JsonValueKind.Number => $"число {value.GetRawText()}",
        JsonValueKind.String => $"строка {Short(value.GetRawText())}",
        JsonValueKind.Array => $"массив из {value.GetArrayLength().ToString(CultureInfo.InvariantCulture)}",
        JsonValueKind.Object => "объект",
        _ => value.ValueKind.ToString()
    };

    private static string Short(string s) => s.Length <= 60 ? s : s[..57] + "...";

    private static void Collect(JsonElement schema, SortedSet<string> found)
    {
        if (schema.ValueKind != JsonValueKind.Object) return;
        foreach (var p in schema.EnumerateObject())
        {
            if (!Checked.Contains(p.Name) && !Annotations.Contains(p.Name) && !p.Name.StartsWith("x-", StringComparison.Ordinal))
                found.Add(p.Name);
            foreach (var child in SubSchemas(p)) Collect(child, found);
        }
    }

    // где внутри схемы лежат вложенные схемы
    private static IEnumerable<JsonElement> SubSchemas(JsonProperty p) => p.Name switch
    {
        "properties" or "definitions" or "$defs" when p.Value.ValueKind == JsonValueKind.Object => p.Value.EnumerateObject().Select(x => x.Value),
        "allOf" or "anyOf" or "oneOf" when p.Value.ValueKind == JsonValueKind.Array => p.Value.EnumerateArray(),
        "items" or "additionalProperties" or "not" => [p.Value],
        _ => []
    };
}
