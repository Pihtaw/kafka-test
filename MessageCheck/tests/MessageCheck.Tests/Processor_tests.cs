using System.Text;
using System.Text.Json;
using MessageCheck;

namespace MessageCheck.Tests;

// текст или байты в base64, valueBase64 для не UTF-8
public record FixtureHeader(string Key, string? Value = null, string? ValueBase64 = null)
{
    public Header ToHeader() => new(Key,
        ValueBase64 is not null ? Convert.FromBase64String(ValueBase64)
        : Value is not null ? Encoding.UTF8.GetBytes(Value)
        : null);
}

public record FixtureCase(
    string Name,
    List<FixtureHeader> Headers,
    string? Payload,
    string ExpectedVerdict,
    string ExpectedReason)
{
    public override string ToString() => Name;

    public KafkaRecord ToRecord() =>
        new(Headers.Select(h => h.ToHeader()).ToList(), null, Payload is null ? null : Encoding.UTF8.GetBytes(Payload));
}

public class ProcessorTests
{
    // валидное сообщение
    private static readonly KafkaRecord ValidRecord = new(
        [new Header(HeaderNames.EventType, "MessageCheck.SampleContracts.OrderCreated"u8.ToArray()), new Header(HeaderNames.PackageVersion, "1.0.0"u8.ToArray())],        "order-1"u8.ToArray(),
        """{"OrderId":1,"Amount":500}"""u8.ToArray());

    private static Processor MakeProcessor(IConverter converter)
        => new(EventCatalog.Default(), converter, TimeSpan.FromMilliseconds(500));

    public static IEnumerable<object[]> Fixtures()
    {
        var json = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "cases.json"));
        var cases = JsonSerializer.Deserialize<List<FixtureCase>>(json, new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
        return cases.Select(c => new object[] { c });
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task Fixture_gives_expected_decision_and_reason(FixtureCase c)
    {
        var decision = await MakeProcessor(new JsonModule()).ProcessAsync(c.ToRecord());

        Assert.Equal(c.ExpectedVerdict, decision.Verdict.ToString());
        Assert.Equal(c.ExpectedReason, decision.Reason.ToString());
        Assert.Equal(c.ExpectedVerdict == "Allowed", decision.CanSend);
    }

    [Fact]
    public async Task Timeout_blocks_sending()
    {
        var decision = await MakeProcessor(new SlowModule()).ProcessAsync(ValidRecord);

        Assert.False(decision.CanSend);
        Assert.Equal(Reason.ModuleTimeout, decision.Reason);
    }

    [Fact]
    public async Task Crash_blocks_sending()
    {
        var decision = await MakeProcessor(new CrashingModule()).ProcessAsync(ValidRecord);

        Assert.False(decision.CanSend);
        Assert.Equal(Reason.ModuleCrashed, decision.Reason);
    }

    [Fact]
    public async Task Null_answer_blocks_sending()
    {
        var decision = await MakeProcessor(new WrongAnswerModule(null)).ProcessAsync(ValidRecord);

        Assert.False(decision.CanSend);
        Assert.Equal(Reason.ModuleInvalidResponse, decision.Reason);
    }

    [Fact]
    public async Task Wrong_type_answer_blocks_sending()
    {
        var decision = await MakeProcessor(new WrongAnswerModule("какая-то строка")).ProcessAsync(ValidRecord);

        Assert.False(decision.CanSend);
        Assert.Equal(Reason.ModuleInvalidResponse, decision.Reason);
    }

    // есть JSON после преобразования
    [Fact]
    public async Task Allowed_record_has_result_json()
    {
        var decision = await MakeProcessor(new JsonModule()).ProcessAsync(ValidRecord);

        Assert.True(decision.CanSend);
        Assert.Equal("""{"OrderId":1,"Amount":500,"Currency":null}""", decision.ResultJson);
    }


    [Fact]
    public async Task Extra_fields_are_skipped_but_shown_in_details()
    {
        var record = ValidRecord with { Payload = """{"OrderId":1,"Amount":500,"Comment":"hi"}"""u8.ToArray() };

        var decision = await MakeProcessor(new JsonModule()).ProcessAsync(record);

        Assert.True(decision.CanSend);
        Assert.Contains("лишние поля: Comment", decision.Details);
    }
}
