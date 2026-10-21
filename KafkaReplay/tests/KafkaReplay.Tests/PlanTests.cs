using System.Text;
using System.Text.Json;
using KafkaReplay;
using MessageCheck;

namespace KafkaReplay.Tests;

// суммы ловят любое изменение, после сохранения и чтения план остается тем же
public class PlanTests
{
    internal static KafkaRecord Record(string key, string payload) => new(
        [new Header("Event-Type", "A.OrderCreated"u8.ToArray()), new Header("Event-PackageVersion", "1.0.0"u8.ToArray())],
        Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(payload));

    internal static PlanFile MakePlan(int items = 3)
    {
        var list = Enumerable.Range(0, items)
            .Select(i => PlanItem.Create(i, 0, 100 + i, 1_700_000_000_000 + i, "A.OrderCreated", Record($"{i}", $$"""{"OrderId":{{i}}}"""), "orders-copy", i % 3))
            .ToList();
        return PlanFile.Create(new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.FromHours(3)), "orders", "plain localhost:9092", "localhost:9092",
            "plain localhost:9092", "localhost:9092", "crc32", null, items + 2,
            new Dictionary<string, int>(StringComparer.Ordinal) { ["отклонено проверкой"] = 1, ["не отправлялось"] = 1 }, list);
    }

    [Fact]
    public void Fresh_plan_verifies() => Assert.Empty(MakePlan().Verify());

    [Fact]
    public void Plan_survives_save_and_load()
    {
        var plan = MakePlan();
        var loaded = JsonSerializer.Deserialize<PlanFile>(JsonSerializer.Serialize(plan, PlanStore.Options), PlanStore.Options)!;
        Assert.Empty(loaded.Verify());
        Assert.Equal(plan.PlanSha256, loaded.PlanSha256);
    }

    [Fact]
    public void Changed_payload_byte_blocks_plan()
    {
        var plan = MakePlan();
        var item = plan.Items[1];
        var tampered = item with { Payload = Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"OrderId":999}""")) };
        var bad = plan with { Items = [plan.Items[0], tampered, plan.Items[2]] };

        var problems = bad.Verify();
        Assert.Contains(problems, p => p.StartsWith("строка 1", StringComparison.Ordinal));
    }

    [Fact]
    public void Changed_target_topic_blocks_plan()
    {
        var plan = MakePlan();
        var bad = plan with { Items = plan.Items.Select(i => i.Index == 0 ? i with { TargetTopic = "orders" } : i).ToList() };
        Assert.NotEmpty(bad.Verify());
    }

    [Fact]
    public void Changed_header_blocks_plan()
    {
        var plan = MakePlan();
        var item = plan.Items[2];
        var headers = item.Headers.Select(h => string.Equals(h.Key, "Event-PackageVersion", StringComparison.Ordinal) ? h with { Value = Convert.ToBase64String("9.9.9"u8.ToArray()) } : h).ToList();
        var bad = plan with { Items = [plan.Items[0], plan.Items[1], item with { Headers = headers }] };
        Assert.NotEmpty(bad.Verify());
    }

    [Fact]
    public void Removed_line_blocks_plan()
    {
        var plan = MakePlan();
        var bad = plan with { Items = [plan.Items[0], plan.Items[1]] };
        Assert.Contains(bad.Verify(), p => p.Contains("всего плана"));
    }

    [Fact]
    public void Changed_target_cluster_in_header_blocks_plan()
    {
        var bad = MakePlan() with { TargetBootstrap = "other:9092" };
        Assert.Contains(bad.Verify(), p => p.Contains("всего плана"));
    }

    [Fact]
    public void Broken_base64_is_reported()
    {
        var plan = MakePlan();
        var bad = plan with { Items = plan.Items.Select(i => i.Index == 0 ? i with { Key = "не base64!" } : i).ToList() };
        Assert.Contains(bad.Verify(), p => p.Contains("base64"));
    }

    [Fact]
    public void Plan_keeps_exact_bytes_including_null_header()
    {
        var record = new KafkaRecord([new Header("Event-Type", "A.X"u8.ToArray()), new Header("empty", null), new Header("bin", [0xFF, 0x00])], null, [1, 2, 3]);
        var item = PlanItem.Create(0, 0, 0, 0, "A.X", record, "t", 0);
        var back = item.ToRecord();

        Assert.Null(back.Key);
        Assert.Null(back.Headers[1].Value);
        Assert.Equal(new byte[] { 0xFF, 0x00 }, back.Headers[2].Value);
        Assert.Equal(new byte[] { 1, 2, 3 }, back.Payload);
    }
}
