using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using MessageCheck;

namespace MessageCheck.Tests;

// придуманный контракт в духе настоящих: Uuid, время, nullable и не-nullable поля, массивы, вложенные классы, рекурсия
public class ForecastEvent
{
    public Guid EventId { get; init; }
    public DateTime AtUtc { get; init; }
    public int CountryId { get; init; }
    public string SolverVersion { get; init; } = "";
    public IReadOnlyList<CourierSlot> Couriers { get; init; } = [];
    public PlanBlock Plan { get; init; } = new();
}

public class CourierSlot
{
    public Guid CourierId { get; init; }
    public int BoxCapacity { get; init; }
}

public class PlanBlock
{
    public string? CurrentStage { get; init; }
    public int? Wave { get; init; }
    public IReadOnlyList<Guid>? TripOrderIds { get; init; }
    public IReadOnlyDictionary<string, long> Counters { get; init; } = new Dictionary<string, long>(StringComparer.Ordinal);
    public PlanBlock? Next { get; init; }
}

// поля, которые System.Text.Json пишет по-особому: строкой, как есть, не всегда или вперемешку с незнакомыми
public class Extras
{
    public TimeOnly OpensAt { get; init; }
    public Uri? Site { get; init; }
    public object? Payload { get; init; }
    public JsonElement Raw { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Comment { get; init; }

    [JsonIgnore]
    public string Secret { get; init; } = "";

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? Rest { get; init; }
}

// схема из C#-контракта: так, как их будут генерировать из контрактов
public class SchemaGeneratorTests
{
    private static readonly JsonObject Schema = SchemaGenerator.Generate(typeof(ForecastEvent));

    private static JsonNode Property(string name) => Schema["properties"]![name]!;
    private static JsonNode Definition<T>(string name) => Schema["definitions"]![typeof(T).FullName!]!["properties"]![name]!;
    private static string Text(JsonNode? node) => node!.ToJsonString();

    [Fact]
    public void Root_lists_all_properties_as_required_and_forbids_extra_fields()
    {
        Assert.Equal(typeof(ForecastEvent).FullName, (string?)Schema["title"]);
        Assert.False((bool)Schema["additionalProperties"]!);
        Assert.Equal("""["EventId","AtUtc","CountryId","SolverVersion","Couriers","Plan"]""", Text(Schema["required"]));
    }

    [Fact]
    public void Simple_types_map_like_generated_schemas()
    {
        Assert.Equal("""{"type":"string","format":"uuid"}""", Text(Property("EventId")));
        Assert.Equal("""{"type":"string","format":"date-time"}""", Text(Property("AtUtc")));
        Assert.Equal("""{"type":"integer","format":"int32"}""", Text(Property("CountryId")));
    }

    [Fact]
    public void Nullable_annotations_decide_null()
    {
        Assert.Equal("""{"type":"string"}""", Text(Property("SolverVersion")));                        // string — null нельзя
        Assert.Equal("""{"type":["string","null"]}""", Text(Definition<PlanBlock>("CurrentStage")));   // string? — можно
        Assert.Equal("""{"type":["integer","null"],"format":"int32"}""", Text(Definition<PlanBlock>("Wave")));
    }

    [Fact]
    public void Collections_dictionaries_and_nested_classes()
    {
        Assert.Equal($$$"""{"type":"array","items":{"$ref":"#/definitions/{{{typeof(CourierSlot).FullName}}}"}}""", Text(Property("Couriers")));
        Assert.Equal("""{"type":["array","null"],"items":{"type":"string","format":"uuid"}}""", Text(Definition<PlanBlock>("TripOrderIds")));
        Assert.Equal("""{"type":"object","additionalProperties":{"type":"integer","format":"int64"}}""", Text(Definition<PlanBlock>("Counters")));
    }

    [Fact]
    public void Recursive_class_is_one_definition_with_nullable_reference()
    {
        Assert.Equal($$"""{"oneOf":[{"type":"null"},{"$ref":"#/definitions/{{typeof(PlanBlock).FullName}}"}]}""", Text(Definition<PlanBlock>("Next")));
        Assert.Equal(2, Schema["definitions"]!.AsObject().Count);   // CourierSlot и PlanBlock — по разу
    }

    // главное: событие, записанное самим контрактом, схему проходит, а null в не-nullable поле — нет

    private static IReadOnlyList<SchemaViolation> Validate(ForecastEvent e)
    {
        using var doc = JsonDocument.Parse(Schema.ToJsonString());
        return SchemaValidator.Validate(doc.RootElement, JsonSerializer.SerializeToUtf8Bytes(e));
    }

    private static readonly ForecastEvent Good = new()
    {
        EventId = Guid.NewGuid(),
        AtUtc = new DateTime(2026, 10, 13, 12, 0, 3, DateTimeKind.Utc),
        CountryId = 643,
        SolverVersion = "171",
        Couriers = [new CourierSlot { CourierId = Guid.NewGuid(), BoxCapacity = 3 }],
        Plan = new PlanBlock { CurrentStage = null, Wave = 0, TripOrderIds = null, Next = new PlanBlock { CurrentStage = "kitchen" } }
    };

    [Fact]
    public void Event_written_by_contract_passes_its_schema() =>
        Assert.Empty(Validate(Good));

    [Fact]
    public void Null_in_not_nullable_field_is_caught()
    {
        var v = Assert.Single(Validate(new ForecastEvent { EventId = Good.EventId, AtUtc = Good.AtUtc, SolverVersion = null!, Couriers = Good.Couriers, Plan = Good.Plan }));
        Assert.Equal("$.SolverVersion", v.Path);
        Assert.True(v.IsWarning);
    }

    [Fact]
    public void Old_contract_without_annotations_does_not_raise_false_alarms() =>
        Assert.Equal("""{"type":["string","null"]}""", Text(SchemaGenerator.Generate(typeof(Oblivious))["properties"]!["Name"]));
}

// исправления: типы, которые пишутся строкой или как есть, и поля [JsonIgnore]/[JsonExtensionData] не дают ложных ⚠
public class SchemaGeneratorSpecialFieldsTests
{
    private static readonly JsonObject Schema = SchemaGenerator.Generate(typeof(Extras));

    private static string Property(string name) => Schema["properties"]![name]!.ToJsonString();

    [Fact]
    public void Types_written_as_strings_are_strings()
    {
        Assert.Equal("""{"type":"string","format":"time"}""", Property("OpensAt"));
        Assert.Equal("""{"type":["string","null"]}""", Property("Site"));
    }

    [Fact]
    public void Object_and_json_element_are_not_constrained()
    {
        Assert.Equal("{}", Property("Payload"));
        Assert.Equal("{}", Property("Raw"));
    }

    [Fact]
    public void Conditional_ignore_is_optional_and_always_ignore_is_absent()
    {
        var required = Schema["required"]!.AsArray().Select(n => (string?)n).ToList();
        Assert.DoesNotContain("Comment", required, StringComparer.Ordinal);
        Assert.NotNull(Schema["properties"]!["Comment"]);
        Assert.Null(Schema["properties"]!["Secret"]);
    }

    [Fact]
    public void Extension_data_allows_unknown_fields() =>
        Assert.True((bool)Schema["additionalProperties"]!);

    [Fact]
    public void Event_with_special_fields_passes_its_schema()
    {
        var e = new Extras
        {
            OpensAt = new TimeOnly(10, 30),
            Site = new Uri("https://example.com/menu"),
            Payload = new { Any = 1 },
            Raw = JsonSerializer.Deserialize<JsonElement>("[1,2]"),
            Comment = null,   // при WhenWritingNull поле не пишется вовсе
            Rest = new Dictionary<string, JsonElement>(StringComparer.Ordinal) { ["Extra"] = JsonSerializer.Deserialize<JsonElement>("true") }
        };
        using var doc = JsonDocument.Parse(Schema.ToJsonString());
        Assert.Empty(SchemaValidator.Validate(doc.RootElement, JsonSerializer.SerializeToUtf8Bytes(e)));
    }
}

#nullable disable
// контракт без пометок nullable (как до C# 8): про null ничего не известно
public class Oblivious
{
    public string Name { get; init; }
}
#nullable restore
