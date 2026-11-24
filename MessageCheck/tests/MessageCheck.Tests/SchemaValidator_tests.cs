using System.Text;
using System.Text.Json;
using MessageCheck;

namespace MessageCheck.Tests;

// сырое событие против JSON-схемы контракта: то, что C#-десериализатор пропускает молча
public class SchemaValidatorTests
{
    private const string Order = """
        {
          "type": "object",
          "additionalProperties": false,
          "required": [ "OrderId", "Stage", "Courier", "Items" ],
          "properties": {
            "OrderId": { "type": "string", "format": "uuid" },
            "At": { "type": "string", "format": "date-time" },
            "Stage": { "type": "string", "enum": [ "kitchen", "delivery" ] },
            "Source": { "type": [ "string", "null" ], "enum": [ "plan", null ] },
            "Courier": { "oneOf": [ { "type": "null" }, { "$ref": "#/definitions/Courier" } ] },
            "Items": { "type": "array", "items": { "$ref": "#/definitions/Item" } }
          },
          "definitions": {
            "Courier": { "type": "object", "required": [ "Capacity" ], "properties": { "Capacity": { "type": "integer", "format": "int32" } } },
            "Item": { "type": "object", "required": [ "Count" ], "properties": { "Count": { "type": "integer" } } }
          }
        }
        """;

    private const string Valid = """
        {"OrderId":"11f0a8b2c3d4e5f6a7b8c9d0e1f20102","At":"2026-10-13T12:00:03Z","Stage":"kitchen","Source":null,
         "Courier":{"Capacity":3},"Items":[{"Count":1},{"Count":2}]}
        """;

    private static IReadOnlyList<SchemaViolation> Check(string schema, string payload)
    {
        using var doc = JsonDocument.Parse(schema);
        return SchemaValidator.Validate(doc.RootElement, Encoding.UTF8.GetBytes(payload));
    }

    // заменить одно поле в правильном событии
    private static string With(string field, string json) =>
        Valid.Replace(field, json, StringComparison.Ordinal);

    [Fact]
    public void Valid_event_has_no_violations() =>
        Assert.Empty(Check(Order, Valid));

    [Fact]
    public void Missing_required_field_is_found()
    {
        var v = Assert.Single(Check(Order, With("\"Stage\":\"kitchen\",", "")));
        Assert.Equal("$.Stage", v.Path);
        Assert.Equal("required", v.Rule);
        Assert.True(v.IsWarning);
    }

    [Fact]
    public void Missing_nullable_field_is_a_note_not_a_warning()
    {
        // Source — ["string","null"]; делаем его обязательным, как это делают генераторы схем из контрактов
        string schema = Order.Replace("\"required\": [ \"OrderId\",", "\"required\": [ \"Source\", \"OrderId\",", StringComparison.Ordinal);
        var v = Assert.Single(Check(schema, With("\"Source\":null,", "")));
        Assert.Equal("$.Source", v.Path);
        Assert.Equal("required-null", v.Rule);
        Assert.False(v.IsWarning);
    }

    [Fact]
    public void Null_where_schema_does_not_allow_it()
    {
        // C# string? прочитает null молча, схема — нет
        var v = Assert.Single(Check(Order, With("\"Stage\":\"kitchen\"", "\"Stage\":null")));
        Assert.Equal("$.Stage", v.Path);
        Assert.Equal("type", v.Rule);
    }

    [Fact]
    public void Value_outside_enum_is_found_and_case_matters()
    {
        var v = Assert.Single(Check(Order, With("\"Stage\":\"kitchen\"", "\"Stage\":\"Kitchen\"")));
        Assert.Equal("enum", v.Rule);
        Assert.Contains("\"Kitchen\"", v.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Null_is_allowed_when_it_is_in_type_and_enum() =>
        Assert.Empty(Check(Order, With("\"Source\":null", "\"Source\":null")));

    [Fact]
    public void Extra_field_is_found_when_schema_forbids_it()
    {
        var v = Assert.Single(Check(Order, With("\"Stage\":\"kitchen\"", "\"Stage\":\"kitchen\",\"Comment\":\"hi\"")));
        Assert.Equal("$.Comment", v.Path);
        Assert.Equal("additionalProperties", v.Rule);
    }

    [Fact]
    public void Ref_and_array_items_report_exact_path()
    {
        var v = Assert.Single(Check(Order, With("{\"Count\":2}", "{\"Count\":\"2\"}")));
        Assert.Equal("$.Items[1].Count", v.Path);
        Assert.Equal("type", v.Rule);
    }

    [Fact]
    public void OneOf_accepts_null_or_object_and_rejects_broken_object()
    {
        Assert.Empty(Check(Order, With("{\"Capacity\":3}", "null")));
        var v = Assert.Single(Check(Order, With("{\"Capacity\":3}", "{}")));
        Assert.Equal("oneOf", v.Rule);
    }

    [Fact]
    public void Uuid_without_dashes_is_valid_as_in_dodo_events()
    {
        Assert.Empty(Check(Order, With("11f0a8b2c3d4e5f6a7b8c9d0e1f20102", "11F0A8B2C3D4E5F6A7B8C9D0E1F20102")));
        Assert.Empty(Check(Order, With("11f0a8b2c3d4e5f6a7b8c9d0e1f20102", "11f0a8b2-c3d4-e5f6-a7b8-c9d0e1f20102")));
        var v = Assert.Single(Check(Order, With("11f0a8b2c3d4e5f6a7b8c9d0e1f20102", "not-a-uuid")));
        Assert.Equal("format", v.Rule);
    }

    [Fact]
    public void Date_time_without_zone_is_format_note_not_warning()
    {
        // время «по местному времени юнита» пишут без пояса — C# прочитает, но это не date-time по RFC 3339
        var v = Assert.Single(Check(Order, With("2026-10-13T12:00:03Z", "2026-10-13T15:00:03")));
        Assert.Equal("$.At", v.Path);
        Assert.Equal("format", v.Rule);
        Assert.False(v.IsWarning);
    }

    [Fact]
    public void Int32_overflow_is_found()
    {
        var v = Assert.Single(Check(Order, With("\"Capacity\":3", "\"Capacity\":3000000000")));
        Assert.Equal("$.Courier", v.Path);   // переполнение внутри oneOf: ни один вариант не подошёл
        Assert.Equal("oneOf", v.Rule);
    }

    [Fact]
    public void Broken_json_is_one_violation() =>
        Assert.Equal("json", Assert.Single(Check(Order, "{\"OrderId\":")).Rule);

    [Fact]
    public void Violations_are_capped()
    {
        string items = string.Join(",", Enumerable.Repeat("{\"Count\":\"x\"}", SchemaValidator.MaxViolations + 50));
        Assert.Equal(SchemaValidator.MaxViolations, Check(Order, With("{\"Count\":1},{\"Count\":2}", items)).Count);
    }

    [Fact]
    public void Unknown_keywords_are_listed_not_silently_ignored()
    {
        using var doc = JsonDocument.Parse("""{"type":"object","properties":{"Name":{"type":"string","minLength":1,"pattern":"^a"}}}""");
        Assert.Equal(new[] { "minLength", "pattern" }, SchemaValidator.Unsupported(doc.RootElement).ToArray());
    }
}
