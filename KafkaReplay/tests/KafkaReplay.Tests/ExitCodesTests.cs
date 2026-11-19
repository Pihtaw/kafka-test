using KafkaReplay;

namespace KafkaReplay.Tests;

public class ExitCodesTests
{
    [Fact]
    public void All_good_is_zero() => Assert.Equal(0, ExitCodes.For(allRead: true, sumMatches: true, rejected: 0));

    [Fact]
    public void Rejected_records_give_one() => Assert.Equal(1, ExitCodes.For(allRead: true, sumMatches: true, rejected: 3));

    [Fact]
    public void Not_all_read_gives_two_even_with_rejected() => Assert.Equal(2, ExitCodes.For(allRead: false, sumMatches: true, rejected: 3));

    [Fact]
    public void Sum_mismatch_gives_two() => Assert.Equal(2, ExitCodes.For(allRead: true, sumMatches: false, rejected: 0));
}
