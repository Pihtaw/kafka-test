using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace MessageCheck;

// JSON-схема события по C#-контракту — так, как схемы будут генерировать из контрактов
// (образец — сгенерированная схема настоящего события):
//   все публичные свойства — в properties и в required (имя как в JSON: имя свойства или [JsonPropertyName]);
//   additionalProperties: false; nullable (string?, int?, Uuid?) — "type": ["…", "null"];
//   Uuid/Guid — string + uuid, DateTime/DateTimeOffset — string + date-time, int — integer + int32, long — integer + int64,
//   decimal/double/float — number, массивы и коллекции — array + items, словари — object + additionalProperties;
//   вложенные классы — в definitions и по $ref: так переживаются и повторы, и рекурсия.
// Nullable берётся из пометок C# 8+ (string против string?). У старых контрактов без пометок поле считается nullable,
// чтобы не было ложных тревог. Тип со своим JsonConverter (кроме Uuid) не ограничиваем: как он пишется в JSON, не знаем
public static class SchemaGenerator
{
    public const string Draft = "http://json-schema.org/draft-04/schema#";

    public static JsonObject Generate(Type eventType)
    {
        var g = new Generation();
        var body = g.ObjectBody(eventType);

        var schema = new JsonObject { ["$schema"] = Draft, ["title"] = eventType.FullName };
        foreach (var (key, value) in body) schema[key] = value?.DeepClone();
        if (g.Definitions.Count > 0)
        {
            var definitions = new JsonObject();
            foreach (var (type, definition) in g.Definitions) definitions[DefinitionName(type)] = definition.DeepClone();
            schema["definitions"] = definitions;
        }
        return schema;
    }

    private static string DefinitionName(Type t) => t.FullName ?? t.Name;

    private sealed class Generation
    {
        private readonly NullabilityInfoContext _nullability = new();
        public Dictionary<Type, JsonObject> Definitions { get; } = [];

        // type, additionalProperties, required, properties одного класса
        public JsonObject ObjectBody(Type type)
        {
            var properties = new JsonObject();
            var required = new JsonArray();
            bool extensionData = false;
            foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                         .Where(x => x.GetMethod is { IsPublic: true } && x.GetIndexParameters().Length == 0))
            {
                // [JsonIgnore] без условия — поле никогда не пишется; WhenWritingNull/WhenWritingDefault — пишется не всегда,
                // поэтому оно есть в схеме, но не обязательно
                var ignore = p.GetCustomAttribute<JsonIgnoreAttribute>();
                if (ignore is { Condition: JsonIgnoreCondition.Always }) continue;
                // [JsonExtensionData] собирает все незнакомые поля — значит, лишние поля у этого класса законны
                if (p.GetCustomAttribute<JsonExtensionDataAttribute>() is not null) { extensionData = true; continue; }

                string name = p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? p.Name;
                properties[name] = Schema(p.PropertyType, _nullability.Create(p));
                if (ignore is null) required.Add(name);
            }
            return new JsonObject
            {
                ["type"] = "object",
                ["additionalProperties"] = extensionData,
                ["required"] = required,
                ["properties"] = properties
            };
        }

        // info — пометки nullable для этого места (свойство, элемент массива); null — пометок нет
        private JsonObject Schema(Type type, NullabilityInfo? info)
        {
            var underlying = Nullable.GetUnderlyingType(type);
            bool nullable = underlying is not null || (!type.IsValueType && info?.ReadState != NullabilityState.NotNull);
            var schema = Core(underlying ?? type, info);
            return nullable ? AllowNull(schema) : schema;
        }

        private JsonObject Core(Type t, NullabilityInfo? info)
        {
            if (Simple(t) is { } simple) return simple;
            if (IsAnyJson(t)) return new JsonObject();   // object, JsonElement, JsonNode — внутри может быть что угодно
            if (t.IsEnum) return t.GetCustomAttribute<JsonConverterAttribute>() is null ? Integer("int32") : new JsonObject();   // по умолчанию enum — число
            if (t.GetCustomAttribute<JsonConverterAttribute>() is not null) return new JsonObject();               // свой конвертер — формат неизвестен

            if ((GenericArguments(t, typeof(IReadOnlyDictionary<,>)) ?? GenericArguments(t, typeof(IDictionary<,>))) is { } kv)
                return new JsonObject { ["type"] = "object", ["additionalProperties"] = Schema(kv[1], Argument(info, 1)) };
            if (t.IsArray)
                return new JsonObject { ["type"] = "array", ["items"] = Schema(t.GetElementType()!, info?.ElementType) };
            if (GenericArguments(t, typeof(IEnumerable<>)) is { } item)
                return new JsonObject { ["type"] = "array", ["items"] = Schema(item[0], Argument(info, 0)) };

            return Reference(t);
        }

        // вложенный класс — один раз в definitions, дальше по $ref (заглушка до заполнения спасает от рекурсии)
        private JsonObject Reference(Type t)
        {
            if (!Definitions.ContainsKey(t))
            {
                Definitions[t] = new JsonObject();
                var body = ObjectBody(t);
                foreach (var (key, value) in body) Definitions[t][key] = value?.DeepClone();
            }
            return new JsonObject { ["$ref"] = $"#/definitions/{DefinitionName(t)}" };
        }

        private static NullabilityInfo? Argument(NullabilityInfo? info, int index) =>
            info is not null && info.GenericTypeArguments.Length > index ? info.GenericTypeArguments[index] : null;

        private static JsonObject? Simple(Type t)
        {
            if (t == typeof(string) || t == typeof(char) || t == typeof(TimeSpan) || t == typeof(byte[])) return String(null);
            if (t == typeof(bool)) return new JsonObject { ["type"] = "boolean" };
            if (t == typeof(int) || t == typeof(short) || t == typeof(byte) || t == typeof(sbyte) || t == typeof(ushort)) return Integer("int32");
            if (t == typeof(long) || t == typeof(uint) || t == typeof(ulong)) return Integer("int64");
            if (t == typeof(decimal) || t == typeof(double) || t == typeof(float)) return new JsonObject { ["type"] = "number" };
            if (t == typeof(DateTime) || t == typeof(DateTimeOffset)) return String("date-time");
            if (t == typeof(DateOnly)) return String("date");
            // эти System.Text.Json тоже пишет строкой, а не объектом
            if (t == typeof(TimeOnly)) return String("time");
            if (t == typeof(Uri) || t == typeof(Version)) return String(null);
            if (t == typeof(Int128) || t == typeof(UInt128)) return new JsonObject { ["type"] = "integer" };
            if (t == typeof(Half)) return new JsonObject { ["type"] = "number" };
            // Uuid из Dodo.Primitives пишется строкой из 32 hex-символов
            if (t == typeof(Guid) || string.Equals(t.Name, "Uuid", StringComparison.Ordinal)) return String("uuid");
            return null;
        }

        private static JsonObject String(string? format) =>
            format is null ? new JsonObject { ["type"] = "string" } : new JsonObject { ["type"] = "string", ["format"] = format };

        private static JsonObject Integer(string format) => new() { ["type"] = "integer", ["format"] = format };

        private static bool IsAnyJson(Type t) =>
            t == typeof(object) || t == typeof(JsonElement) || t == typeof(JsonDocument) || typeof(JsonNode).IsAssignableFrom(t);

        // "type": "string" → ["string", "null"]; $ref → oneOf с null; пустая схема и так принимает null
        private static JsonObject AllowNull(JsonObject schema)
        {
            if (schema["type"] is JsonValue type)
            {
                schema["type"] = new JsonArray(type.GetValue<string>(), "null");
                return schema;
            }
            if (schema.ContainsKey("$ref"))
                return new JsonObject { ["oneOf"] = new JsonArray(new JsonObject { ["type"] = "null" }, schema) };
            return schema;
        }

        // аргументы generic-интерфейса, который тип реализует (или которым является)
        private static Type[]? GenericArguments(Type t, Type definition)
        {
            if (t == typeof(string)) return null;
            if (t.IsGenericType && t.GetGenericTypeDefinition() == definition) return t.GetGenericArguments();
            return Array.Find(t.GetInterfaces(), i => i.IsGenericType && i.GetGenericTypeDefinition() == definition)?.GetGenericArguments();
        }
    }
}
