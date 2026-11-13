using KafkaReplay;

namespace KafkaReplay.Tests;

// окно по времени [from; to) против диапазона offsets [Start; End), который дала Kafka, и хвост после End
public class WindowTests
{
    private const long From = 1_000, To = 2_000, End = 50;

    [Theory]
    [InlineData(10, 1_500, WindowPlace.Inside)]          // в диапазоне, время в окне
    [InlineData(10, 2_500, WindowPlace.OutsideWindow)]   // в диапазоне, время после окна — «чужое внутри»
    [InlineData(10, 500, WindowPlace.OutsideWindow)]     // в диапазоне, время до окна
    [InlineData(50, 1_500, WindowPlace.LateAfterEnd)]    // ровно End, время в окне — «наше снаружи»
    [InlineData(70, 1_999, WindowPlace.LateAfterEnd)]
    [InlineData(70, 2_000, WindowPlace.AfterEnd)]        // to не входит в окно
    [InlineData(70, 999, WindowPlace.AfterEnd)]
    public void Place_of_record(long offset, long ts, WindowPlace expected) =>
        Assert.Equal(expected, Window.Place(offset, End, ts, From, To));

    [Theory]
    [InlineData(0, 50)]                  // --tail 0: только окно
    [InlineData(10, 60)]                 // окно + 10 записей хвоста
    [InlineData(1_000, 100)]             // хвост длиннее партиции — до high
    [InlineData(long.MaxValue, 100)]     // --tail all
    public void Read_to(long tail, long expected) =>
        Assert.Equal(expected, Window.ReadTo(new PartitionPlan("t", 0, Low: 0, High: 100, Start: 10, End: 50), tail));

    [Fact]
    public void Tail_stats_count_late_and_skipped_by_partition()
    {
        var stats = new TailStats();
        stats.Add(0, WindowPlace.LateAfterEnd);
        stats.Add(0, WindowPlace.LateAfterEnd);
        stats.Add(0, WindowPlace.AfterEnd);
        stats.Add(1, WindowPlace.AfterEnd);
        stats.Add(1, WindowPlace.Inside);   // не хвост — не считается

        Assert.Equal(2, stats.Late(0));
        Assert.Equal(1, stats.Skipped(0));
        Assert.Equal(1, stats.Skipped(1));
        Assert.Equal(2, stats.TotalLate);
        Assert.Equal(2, stats.TotalSkipped);
    }

    [Fact]
    public void Tally_accepts_tail_and_counts_skipped_records()
    {
        var plan = new[] { new PartitionPlan("t", 0, Low: 0, High: 100, Start: 10, End: 12) };
        var readTo = new Dictionary<int, long> { [0] = 15 };
        var tally = new ReadTally(plan, readTo);
        foreach (long o in new long[] { 10, 11, 12, 13, 14 }) tally.Add(0, o);

        Assert.Equal(0, tally.OutOfRange);           // 12..14 — хвост, это не выход за план
        Assert.True(tally.Matches(3, skippedInTail: 2));
        Assert.False(tally.Matches(3));               // пропущенные в хвосте нужно учесть
    }

    [Theory]
    [InlineData("0", 0L)]
    [InlineData("10000", 10_000L)]
    [InlineData("all", long.MaxValue)]
    [InlineData("ALL", long.MaxValue)]
    public void Tail_option_parses(string text, long expected)
    {
        Assert.True(CliOptions.TryParseTail(text, out var tail));
        Assert.Equal(expected, tail);
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("много")]
    [InlineData("1.5")]
    public void Tail_option_rejects_garbage(string text) =>
        Assert.False(CliOptions.TryParseTail(text, out _));

    [Fact]
    public void Tail_default_and_from_args()
    {
        string[] baseArgs = ["--topic", "t", "--from", "2026-10-28 14:21:10+00:00", "--to", "2026-10-28 14:21:30+00:00", "--bootstrap", "localhost:9092"];

        Assert.True(CliOptions.TryParse(baseArgs, DateTimeOffset.UnixEpoch, out var byDefault, out _));
        Assert.Equal(CliOptions.DefaultTail, byDefault!.Tail);

        Assert.True(CliOptions.TryParse([.. baseArgs, "--tail", "all"], DateTimeOffset.UnixEpoch, out var all, out _));
        Assert.Equal(long.MaxValue, all!.Tail);

        Assert.False(CliOptions.TryParse([.. baseArgs, "--tail", "-5"], DateTimeOffset.UnixEpoch, out _, out var error));
        Assert.Contains("--tail", error, StringComparison.Ordinal);
    }
}
