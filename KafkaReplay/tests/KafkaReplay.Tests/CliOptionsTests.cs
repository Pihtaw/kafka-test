using KafkaReplay;

namespace KafkaReplay.Tests;

public class CliOptionsTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 14, 15, 42, 0, TimeSpan.FromHours(3));

    private static string[] Args(params string[] extra) =>
        ["--topic", "orders", "--from", "2026-10-14 15:00:00+03:00", "--to", "2026-10-14 16:00:00+03:00", .. extra];

    private static string ParseError(string[] args)
    {
        Assert.False(CliOptions.TryParse(args, Now, out var options, out var error));
        Assert.Null(options);
        return error!;
    }

    [Fact]
    public void Bootstrap_is_parsed()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "localhost:9092"), Now, out var o, out _));

        Assert.Equal("orders", o!.Topic);
        Assert.Equal(new DateTimeOffset(2026, 10, 14, 15, 0, 0, TimeSpan.FromHours(3)), o.From);
        Assert.Equal(new DateTimeOffset(2026, 10, 14, 16, 0, 0, TimeSpan.FromHours(3)), o.To);
        Assert.Equal("localhost:9092", o.Bootstrap);
        Assert.Null(o.SourceEnv);
    }

    [Fact]
    public void Source_env_is_parsed()
    {
        Assert.True(CliOptions.TryParse(Args("--source-env", "REPLAY_SOURCE"), Now, out var o, out _));

        Assert.Equal("REPLAY_SOURCE", o!.SourceEnv);
        Assert.Null(o.Bootstrap);
    }

    [Fact]
    public void Contracts_path_is_parsed()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "x:1", "--contracts", "/tmp/registry/Dodo.EventRegistry.dll"), Now, out var o, out _));
        Assert.Equal("/tmp/registry/Dodo.EventRegistry.dll", o!.Contracts);
    }

    [Fact]
    public void Without_contracts_built_in_samples_are_used()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "x:1"), Now, out var o, out _));
        Assert.Null(o!.Contracts);
    }

    [Fact]
    public void Out_dir_by_default_has_date_and_time()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "localhost:9092"), Now, out var o, out _));
        Assert.Equal("run-20261014-154200", o!.OutDir);
    }

    [Fact]
    public void Out_dir_can_be_set()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "localhost:9092", "--out", "run-01"), Now, out var o, out _));
        Assert.Equal("run-01", o!.OutDir);
    }

    [Fact]
    public void Needs_bootstrap_or_source_env() =>
        Assert.Contains("--bootstrap или --source-env", ParseError(Args()));

    [Fact]
    public void Bootstrap_and_source_env_together_are_not_allowed() =>
        Assert.Contains("вместе нельзя", ParseError(Args("--bootstrap", "localhost:9092", "--source-env", "REPLAY_SOURCE")));

    [Fact]
    public void Topic_is_required() =>
        Assert.Contains("--topic", ParseError(["--from", "2026-10-14 15:00:00+03:00", "--to", "2026-10-14 16:00:00+03:00", "--bootstrap", "x:1"]));

    [Fact]
    public void Time_without_offset_is_rejected() =>
        Assert.Contains("--from", ParseError(["--topic", "t", "--from", "2026-10-14 15:00:00", "--to", "2026-10-14 16:00:00+03:00", "--bootstrap", "x:1"]));

    [Fact]
    public void From_must_be_before_to() =>
        Assert.Contains("раньше", ParseError(["--topic", "t", "--from", "2026-10-14 16:00:00+03:00", "--to", "2026-10-14 15:00:00+03:00", "--bootstrap", "x:1"]));

    [Fact]
    public void Unknown_parameter_is_rejected() =>
        Assert.Contains("--verbose", ParseError(Args("--bootstrap", "x:1", "--verbose", "yes")));

    [Fact]
    public void Missing_value_is_rejected() =>
        Assert.Contains("нет значения у --out", ParseError(Args("--bootstrap", "x:1", "--out")));

    [Fact]
    public void Same_parameter_twice_is_rejected() =>
        Assert.Contains("дважды", ParseError(Args("--bootstrap", "x:1", "--bootstrap", "y:2")));

    // --- план и отправка ---

    [Fact]
    public void Dry_run_is_default_mode()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "x:1"), Now, out var o, out _));
        Assert.Equal(RunMode.DryRun, o!.Mode);
        Assert.Null(o.Routes);
    }

    [Fact]
    public void Routes_with_target_are_parsed()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "x:1", "--routes", "routes.json", "--target-bootstrap", "y:1", "--partitioner", "murmur2"), Now, out var o, out var e), e);
        Assert.Equal("routes.json", o!.Routes);
        Assert.Equal("y:1", o.TargetBootstrap);
        Assert.Equal(PartitionStrategy.Murmur2, o.Partitioner);
    }

    [Fact]
    public void Default_partitioner_is_crc32()
    {
        Assert.True(CliOptions.TryParse(Args("--bootstrap", "x:1", "--routes", "r.json", "--target-env", "REPLAY_TARGET"), Now, out var o, out _));
        Assert.Equal(PartitionStrategy.Crc32, o!.Partitioner);
        Assert.Equal("REPLAY_TARGET", o.TargetEnv);
    }

    [Fact]
    public void Routes_need_target() =>
        Assert.Contains("нужна цель", ParseError(Args("--bootstrap", "x:1", "--routes", "r.json")));

    [Fact]
    public void Target_without_routes_is_rejected() =>
        Assert.Contains("без --routes", ParseError(Args("--bootstrap", "x:1", "--target-bootstrap", "y:1")));

    [Fact]
    public void Unknown_partitioner_is_rejected() =>
        Assert.Contains("crc32, murmur2, keep", ParseError(Args("--bootstrap", "x:1", "--routes", "r.json", "--target-bootstrap", "y:1", "--partitioner", "random")));

    [Fact]
    public void Execute_is_parsed()
    {
        Assert.True(CliOptions.TryParse(["--execute", "--plan", "/tmp/run-1/plan.json", "--target-bootstrap", "y:1"], Now, out var o, out var e), e);
        Assert.Equal(RunMode.Execute, o!.Mode);
        Assert.Equal("/tmp/run-1/plan.json", o.Plan);
        Assert.Equal("y:1", o.TargetBootstrap);
    }

    [Fact]
    public void Execute_needs_plan() =>
        Assert.Contains("--plan", ParseError(["--execute", "--target-bootstrap", "y:1"]));

    [Fact]
    public void Execute_needs_target() =>
        Assert.Contains("нужна цель", ParseError(["--execute", "--plan", "p.json"]));

    [Fact]
    public void Execute_does_not_read_source_again() =>
        Assert.Contains("только по plan.json", ParseError(["--execute", "--plan", "p.json", "--target-bootstrap", "y:1", "--topic", "orders"]));
}
