using CompatCheck;
using Contracts;
using Xunit;

namespace CompatCheck.Tests;

public class CompatTests
{
    private static ContractVersion V(int n) => OrderCreatedVersions.All.Single(v => v.Version == n);

    private static async Task<CheckResult> Read(int producer, string @case, int consumer, ConsumerMode mode)
    {
        var e = EventFactory.For(V(producer)).Single(x => x.Case == @case);
        return await Compat.CheckAsync(e, V(consumer), mode);
    }

    [Fact]
    public void Generator_made_six_versions()
    {
        Assert.Equal(6, OrderCreatedVersions.All.Count);
    }

    [Fact]
    public void V3_has_no_Comment_and_V4_Quantity_is_long()
    {
        Assert.True(V(2).Type.GetProperty("Comment") is not null);
        Assert.True(V(3).Type.GetProperty("Comment") is null);
        Assert.Equal(typeof(int), V(3).Type.GetProperty("Quantity")!.PropertyType);
        Assert.Equal(typeof(long), V(4).Type.GetProperty("Quantity")!.PropertyType);
    }

    // та же версия читает свои события без потерь

    [Fact]
    public async Task Same_version_reads_everything_without_loss()
    {
        foreach (var mode in new[] { Compat.Lenient, Compat.Strict })
            foreach (var v in OrderCreatedVersions.All)
                foreach (var e in EventFactory.For(v))
                    Assert.Equal(Outcome.Ok, (await Compat.CheckAsync(e, v, mode)).Outcome);
    }

    // добавили поле

    [Fact]
    public async Task Added_field_is_ignored_by_lenient_consumer()
    {
        Assert.Equal(Outcome.ExtraIgnored, (await Read(2, "обычное", 1, Compat.Lenient)).Outcome);
    }

    [Fact]
    public async Task Added_field_breaks_strict_consumer()
    {
        Assert.Equal(Outcome.Exploded, (await Read(2, "обычное", 1, Compat.Strict)).Outcome);
    }

    // удалили поле: (подставить default)

    [Fact]
    public async Task Removed_field_silently_becomes_default()
    {
        var r = await Read(3, "обычное", 2, Compat.Lenient);
        Assert.Equal(Outcome.SilentDefault, r.Outcome);
        Assert.Contains("comment", r.Details);
    }

    // int -> long: ломается только на больших значениях

    [Fact]
    public async Task Int_to_long_is_fine_while_value_fits_int()
    {
        Assert.Equal(Outcome.Ok, (await Read(4, "обычное", 3, Compat.Lenient)).Outcome);
    }

    [Fact]
    public async Task Int_to_long_explodes_on_value_above_int_max()
    {
        Assert.Equal(Outcome.Exploded, (await Read(4, "Quantity = int.MaxValue + 1", 3, Compat.Lenient)).Outcome);
        Assert.Equal(Outcome.Exploded, (await Read(4, "Quantity = int.MinValue − 1", 3, Compat.Lenient)).Outcome);
    }

    // int -> double: ломается только на дробных и огромных

    [Fact]
    public async Task Int_to_double_explodes_on_fraction()
    {
        Assert.Equal(Outcome.Exploded, (await Read(5, "Amount = 1.5", 4, Compat.Lenient)).Outcome);
    }

    [Fact]
    public async Task Int_to_double_whole_number_is_written_as_integer_and_reads_fine()
    {
        // System.Text.Json пишет 2.0 как "2", поэтому старый int его читает
        Assert.Equal(Outcome.Ok, (await Read(5, "Amount = 2.0", 4, Compat.Lenient)).Outcome);
    }

    // string -> int: ломается

    [Fact]
    public async Task String_to_int_explodes_on_every_event()
    {
        foreach (var e in EventFactory.For(V(6)))
            Assert.Equal(Outcome.Exploded, (await Compat.CheckAsync(e, V(5), Compat.Lenient)).Outcome);
    }
}
