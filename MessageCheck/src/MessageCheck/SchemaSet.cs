using System.Text.Json;

namespace MessageCheck;

// схема одного события и откуда она
public sealed record EventSchema(string Name, string File, JsonElement Schema, IReadOnlySet<string> Unsupported);

// JSON-схемы контрактов из папки: один файл *.json — одна схема события.
// Имя схемы — title (генераторы пишут туда имя класса), без title — имя файла без .schema.json / .json.
// Тип из Event-Type ищем сначала по полному имени, потом по имени класса (после последней точки)
public sealed class SchemaSet
{
    private const string SchemaSuffix = ".schema.json";
    private readonly Dictionary<string, EventSchema> _byName;

    private SchemaSet(string folder, Dictionary<string, EventSchema> byName)
    {
        Folder = folder;
        _byName = byName;
    }

    public string Folder { get; }
    public int Count => _byName.Count;

    public static bool TryLoad(string folder, out SchemaSet? schemas, out string? error)
    {
        schemas = null;
        string full = Path.GetFullPath(folder);
        if (!Directory.Exists(full)) { error = $"нет папки со схемами {full}"; return false; }

        // с учётом регистра — как Event-Type в Dodo.Kafka и в EventCatalog
        var byName = new Dictionary<string, EventSchema>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(full, "*.json").Order(StringComparer.Ordinal))
        {
            if (!TryRead(file, out var schema, out error)) return false;
            string name = NameOf(file, schema);
            if (byName.TryGetValue(name, out var other))
            {
                error = $"схема {name} есть в двух файлах: {Path.GetFileName(other.File)} и {Path.GetFileName(file)}";
                return false;
            }
            byName[name] = new EventSchema(name, file, schema, SchemaValidator.Unsupported(schema));
        }
        if (byName.Count == 0) { error = $"в {full} нет файлов *.json со схемами"; return false; }

        schemas = new SchemaSet(full, byName);
        error = null;
        return true;
    }

    public EventSchema? Find(string eventType)
    {
        if (_byName.TryGetValue(eventType, out var byFullName)) return byFullName;
        int dot = eventType.LastIndexOf('.');
        return dot >= 0 && _byName.TryGetValue(eventType[(dot + 1)..], out var byClassName) ? byClassName : null;
    }

    // null — схемы для этого типа нет, проверять нечем
    public IReadOnlyList<SchemaViolation>? Check(string eventType, byte[] payload) =>
        Find(eventType) is { } s ? SchemaValidator.Validate(s.Schema, payload) : null;

    public string Describe()
    {
        var unsupported = _byName.Values.SelectMany(s => s.Unsupported).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        string text = $"{Count} схем из {Folder}";
        return unsupported.Count == 0 ? text : $"{text}; ⚠ эти ключевые слова не проверяются: {string.Join(", ", unsupported)}";
    }

    private static bool TryRead(string file, out JsonElement schema, out string? error)
    {
        schema = default;
        try
        {
            using var doc = JsonDocument.Parse(File.ReadAllBytes(file));
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                error = $"{Path.GetFileName(file)}: схема должна быть JSON-объектом";
                return false;
            }
            schema = doc.RootElement.Clone();   // копия живёт и после закрытия документа
            error = null;
            return true;
        }
        catch (JsonException e)
        {
            error = $"{Path.GetFileName(file)}: не JSON ({e.Message})";
            return false;
        }
    }

    private static string NameOf(string file, JsonElement schema)
    {
        if (schema.TryGetProperty("title", out var title) && title.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(title.GetString()))
            return title.GetString()!;
        string name = Path.GetFileName(file);
        return name.EndsWith(SchemaSuffix, StringComparison.OrdinalIgnoreCase) ? name[..^SchemaSuffix.Length] : Path.GetFileNameWithoutExtension(name);
    }
}
