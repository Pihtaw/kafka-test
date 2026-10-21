using KafkaReplay;
using MessageCheck;

namespace KafkaReplay.Tests;

public class ExecutorTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.FromHours(3));

    private sealed class FakeSender(Func<PlanItem, DeliveryStatus> answer) : IPlanSender
    {
        public List<int> Sent { get; } = [];

        public Task<IReadOnlyList<SendOutcome>> SendAsync(IReadOnlyList<PlanItem> items)
        {
            Sent.AddRange(items.Select(i => i.Index));
            IReadOnlyList<SendOutcome> result = items.Select(i =>
            {
                var s = answer(i);
                return new SendOutcome(i.Index, s, s == DeliveryStatus.Written ? i.TargetPartition : null, s == DeliveryStatus.Written ? 1000 + i.Index : null, null);
            }).ToList();
            return Task.FromResult(result);
        }
    }

    private static async Task<ExecutionFile> Run(PlanFile plan, ExecutionFile? previous, FakeSender sender, List<ExecutionFile>? saves = null) =>
        await Executor.RunAsync(plan, previous, sender, s => saves?.Add(s), Now).ConfigureAwait(false);

    [Fact]
    public async Task All_written()
    {
        var plan = PlanTests.MakePlan();
        var state = await Run(plan, null, new FakeSender(_ => DeliveryStatus.Written));

        Assert.Equal(3, state.Items.Count);
        Assert.True(state.Items.All(s => s.Status == DeliveryStatus.Written));
        Assert.Equal(1001, state.Items[1].Offset);
    }

    [Fact]
    public async Task Each_kafka_answer_becomes_its_status()
    {
        var plan = PlanTests.MakePlan();
        var state = await Run(plan, null, new FakeSender(i => i.Index switch
        {
            0 => DeliveryStatus.Written,
            1 => DeliveryStatus.NotWritten,
            _ => DeliveryStatus.Unknown
        }));

        Assert.Equal([DeliveryStatus.Written, DeliveryStatus.NotWritten, DeliveryStatus.Unknown], state.Items.Select(s => s.Status).ToArray());
    }

    [Fact]
    public async Task Second_run_resends_only_not_written()
    {
        var plan = PlanTests.MakePlan();
        var first = await Run(plan, null, new FakeSender(i => i.Index switch
        {
            0 => DeliveryStatus.Written,
            1 => DeliveryStatus.NotWritten,
            _ => DeliveryStatus.Unknown
        }));

        var sender = new FakeSender(_ => DeliveryStatus.Written);
        var second = await Run(plan, first, sender);

        Assert.Equal([1], sender.Sent);
        Assert.Equal(DeliveryStatus.Written, second.Items[1].Status);
        Assert.Equal(DeliveryStatus.Unknown, second.Items[2].Status);
    }

    [Fact]
    public async Task Unknown_is_never_resent_automatically()
    {
        var plan = PlanTests.MakePlan(1);
        var first = await Run(plan, null, new FakeSender(_ => DeliveryStatus.Unknown));
        var sender = new FakeSender(_ => DeliveryStatus.Written);

        await Run(plan, first, sender);
        await Run(plan, first, sender);

        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task Batch_is_marked_unknown_before_sending()
    {
        var plan = PlanTests.MakePlan();
        var saves = new List<ExecutionFile>();
        await Run(plan, null, new FakeSender(_ => DeliveryStatus.Written), saves);

        Assert.True(saves.Count >= 2);
        Assert.True(saves[0].Items.All(s => s.Status == DeliveryStatus.Unknown));
        Assert.True(saves[^1].Items.All(s => s.Status == DeliveryStatus.Written));
    }

    [Fact]
    public async Task State_of_other_plan_is_refused()
    {
        var plan = PlanTests.MakePlan();
        var foreign = new ExecutionFile("другой-план", [], []);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Run(plan, foreign, new FakeSender(_ => DeliveryStatus.Written)));
    }

    // --- побайтовая сверка ---

    private static FetchedRecord Fetched(PlanItem item)
    {
        var r = item.ToRecord();
        return new FetchedRecord(r.Key, r.Headers, r.Payload, item.TimestampMs, "CreateTime");
    }

    [Fact]
    public void Same_bytes_match()
    {
        var item = PlanTests.MakePlan().Items[0];
        var v = ByteCompare.Compare(item, Fetched(item));
        Assert.True(v.Matches);
        Assert.Empty(v.Differences);
    }

    [Fact]
    public void Missing_record_does_not_match() =>
        Assert.False(ByteCompare.Compare(PlanTests.MakePlan().Items[0], null).Matches);

    [Fact]
    public void Different_payload_is_found()
    {
        var item = PlanTests.MakePlan().Items[0];
        var actual = Fetched(item) with { Payload = "{}"u8.ToArray() };
        var v = ByteCompare.Compare(item, actual);
        Assert.False(v.Matches);
        Assert.Contains("тело отличается", v.Differences, StringComparer.Ordinal);
    }

    [Fact]
    public void Extra_header_is_found()
    {
        // например заголовок Replicated
        var item = PlanTests.MakePlan().Items[0];
        var actual = Fetched(item);
        actual = actual with { Headers = [.. actual.Headers, new Header("Replicated", "true"u8.ToArray())] };
        Assert.False(ByteCompare.Compare(item, actual).Matches);
    }

    [Fact]
    public void Different_time_is_shown_but_does_not_fail()
    {
        var item = PlanTests.MakePlan().Items[0];
        var v = ByteCompare.Compare(item, Fetched(item) with { TimestampMs = 1, TimestampType = "LogAppendTime" });
        Assert.True(v.Matches);
        Assert.Contains(v.Differences, d => d.Contains("LogAppendTime"));
    }
}
