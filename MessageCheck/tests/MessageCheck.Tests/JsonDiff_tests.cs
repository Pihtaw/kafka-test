using System.Text;
using MessageCheck;

namespace MessageCheck.Tests;

// отличия события от того, что увидит сервис после чтения контрактом
public class JsonDiffTests
{
    private static IReadOnlyList<Difference> Diff(string source, string result) => JsonDiff.Compare(Encoding.UTF8.GetBytes(source), result);

    [Fact]
    public void Same_json_has_no_differences() =>
        Assert.Empty(Diff("""{"OrderId":1,"Amount":500}""", """{"Amount":500,"OrderId":1}"""));

    [Fact]
    public void Extra_field_is_found()
    {
        var d = Assert.Single(Diff("""{"OrderId":1,"Comment":"hi"}""", """{"OrderId":1}"""));
        Assert.Equal(DifferenceKind.Extra, d.Kind);
        Assert.Equal("$.Comment", d.Path);
        Assert.False(d.IsWarning);
    }

    [Fact]
    public void Missing_optional_field_becomes_null()
    {
        // Currency появился в пакете 1.1.0; событие из 1.0.0 его не содержит
        var d = Assert.Single(Diff("""{"OrderId":1}""", """{"OrderId":1,"Currency":null}"""));
        Assert.Equal(DifferenceKind.MissingNull, d.Kind);
        Assert.False(d.IsWarning);
    }

    [Fact]
    public void Missing_number_silently_becomes_zero_and_is_a_warning()
    {
        // поле удалили или не прислали, сервис молча видит 0
        var d = Assert.Single(Diff("""{"OrderId":1}""", """{"OrderId":1,"Quantity":0}"""));
        Assert.Equal(DifferenceKind.MissingDefault, d.Kind);
        Assert.True(d.IsWarning);
        Assert.Contains("молча увидит 0", d.Describe());
    }

    [Fact]
    public void Same_number_written_differently_is_not_a_difference() =>
        Assert.Empty(Diff("""{"Amount":500.0}""", """{"Amount":500}"""));

    [Fact]
    public void Changed_value_is_found()
    {
        var d = Assert.Single(Diff("""{"At":"2026-10-15T10:00:00+03:00"}""", """{"At":"2026-10-15T07:00:00Z"}"""));
        Assert.Equal(DifferenceKind.Changed, d.Kind);
        Assert.True(d.IsWarning);
    }

    [Fact]
    public void Nested_objects_and_arrays_are_compared()
    {
        var diffs = Diff(
            """{"Lines":[{"Product":"pizza","Note":"x"}]}""",
            """{"Lines":[{"Product":"pizza","Quantity":0}]}""");

        Assert.Contains(diffs, d => string.Equals(d.Path, "$.Lines[0].Note", StringComparison.Ordinal) && d.Kind == DifferenceKind.Extra);
        Assert.Contains(diffs, d => string.Equals(d.Path, "$.Lines[0].Quantity", StringComparison.Ordinal) && d.Kind == DifferenceKind.MissingDefault);
    }

    [Fact]
    public void No_differences_reason_names_field_count() =>
    Assert.Contains("все поля события (2)", JsonDiff.NoDifferencesReason(Encoding.UTF8.GetBytes("""{"OrderId":1,"Amount":500}""")));

    [Fact]
    public void Not_a_uuid_in_other_case_is_still_a_change()
    {
        var d = Diff("""{"Id":"11AAAAAAAABBBBBBBBCCCCCC"}""", """{"Id":"11aaaaaaaabbbbbbbbcccccc"}""");
        Assert.Equal(DifferenceKind.Changed, Assert.Single(d).Kind);
    }

    [Fact]
    public void Text_in_other_case_is_still_a_change()
    {
        var d = Diff("""{"Name":"Moscow"}""", """{"Name":"moscow"}""");
        Assert.Equal(DifferenceKind.Changed, Assert.Single(d).Kind);
    }
}
