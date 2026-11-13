using KafkaReplay;

namespace KafkaReplay.Tests;

// сверка чтения: считает независимо от отчёта и ловит потери, повторы и выход за план
public class ReadTallyTests
{
    private static readonly PartitionPlan[] Plan =
    [
        new("t", 0, Low: 0, High: 100, Start: 10, End: 13),   // offsets 10, 11, 12
        new("t", 1, Low: 0, High: 100, Start: 50, End: 52),   // offsets 50, 51
    ];

    [Fact]
    public void Everything_in_plan_matches()
    {
        var tally = new ReadTally(Plan);
        foreach (var (p, o) in new[] { (0, 10L), (0, 11L), (1, 50L), (0, 12L), (1, 51L) }) tally.Add(p, o);

        Assert.True(tally.Matches(5));
        Assert.Equal(3, tally.ReadIn(0));
        Assert.Equal(2, tally.ReadIn(1));
    }

    [Fact]
    public void Report_lost_a_record()
    {
        var tally = new ReadTally(Plan);
        tally.Add(0, 10);
        tally.Add(0, 11);

        Assert.False(tally.Matches(1));   // в цикле 2, в отчёте 1
    }

    [Fact]
    public void Same_offset_twice_is_a_duplicate()
    {
        var tally = new ReadTally(Plan);
        tally.Add(0, 10);
        tally.Add(0, 10);

        Assert.Equal(1, tally.Duplicates);
        Assert.False(tally.Matches(2));
    }

    [Theory]
    [InlineData(0, 9)]    // до Start
    [InlineData(0, 13)]   // End не включается
    [InlineData(2, 10)]   // партиции нет в плане
    public void Outside_plan_is_caught(int partition, long offset)
    {
        var tally = new ReadTally(Plan);
        tally.Add(partition, offset);

        Assert.Equal(1, tally.OutOfRange);
        Assert.False(tally.Matches(1));
    }
}
