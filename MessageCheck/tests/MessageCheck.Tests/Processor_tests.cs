using System.Text.Json;
using MessageCheck;

namespace MessageCheck.Tests;

public record FixtureCase(
    string Name,
    Dictionary<string, string> Headers,
    string Payload,
    string ExpectedVerdict,
    string ExpectedReason)
{
    public override string ToString() => Name;
}

public class ProcessorTests
{
    // валидное сообщение
    private static readonly KafkaRecord ValidRecord = new(
        new() { ["eventType"] = "OrderCreated", ["version"] = "1" },
        "order-1",
        """{"orderId":1,"amount":500}""");

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
        var processor = MakeProcessor(new JsonModule());

        var decision = await processor.ProcessAsync(new KafkaRecord(c.Headers, null, c.Payload));

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
        Assert.Equal("""{"orderId":1,"amount":500}""", decision.ResultJson);
    }
}
