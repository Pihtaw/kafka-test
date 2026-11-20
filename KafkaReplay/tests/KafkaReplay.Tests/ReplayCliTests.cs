using KafkaReplay;

namespace KafkaReplay.Tests;

// разбор командной строки: replay check … / replay execute …
public class ReplayCliTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 14, 15, 42, 0, TimeSpan.FromHours(3));

    private static string[] Check(params string[] extra) =>
        ["check", "--topic", "orders", "--from", "2026-10-14 15:00:00+03:00", "--to", "2026-10-14 16:00:00+03:00", .. extra];

    private static CliOptions Parsed(string[] args)
    {
        Assert.True(new ReplayCli(Now).TryParse(args, out var options, out var error), error);
        return options!;
    }

    private static string ParseError(string[] args)
    {
        Assert.False(new ReplayCli(Now).TryParse(args, out var options, out var error));
        Assert.Null(options);
        return error!;
    }

    // --- check: источник и окно ---

    [Fact]
    public void Check_with_bootstrap_is_parsed()
    {
        var o = Parsed(Check("--bootstrap", "localhost:9092"));

        Assert.Equal(RunMode.DryRun, o.Mode);
        Assert.Equal("orders", o.Topic);
        Assert.Equal(new DateTimeOffset(2026, 10, 14, 15, 0, 0, TimeSpan.FromHours(3)), o.From);
        Assert.Equal(new DateTimeOffset(2026, 10, 14, 16, 0, 0, TimeSpan.FromHours(3)), o.To);
        Assert.Equal("localhost:9092", o.Bootstrap);
        Assert.Null(o.SourceEnv);
        Assert.Null(o.Routes);
    }

    [Fact]
    public void Source_env_is_parsed()
    {
        var o = Parsed(Check("--source-env", "REPLAY_SOURCE"));
        Assert.Equal("REPLAY_SOURCE", o.SourceEnv);
        Assert.Null(o.Bootstrap);
    }

    [Fact]
    public void Contracts_path_is_parsed() =>
        Assert.Equal("/tmp/registry/Dodo.EventRegistry.dll", Parsed(Check("--bootstrap", "x:1", "--contracts", "/tmp/registry/Dodo.EventRegistry.dll")).Contracts);

    [Fact]
    public void Without_contracts_built_in_samples_are_used() =>
        Assert.Null(Parsed(Check("--bootstrap", "x:1")).Contracts);

    [Fact]
    public void Out_dir_by_default_has_date_and_time() =>
        Assert.Equal("run-20261014-154200", Parsed(Check("--bootstrap", "x:1")).OutDir);

    [Fact]
    public void Out_dir_can_be_set() =>
        Assert.Equal("run-01", Parsed(Check("--bootstrap", "x:1", "--out", "run-01")).OutDir);

    [Fact]
    public void Tail_default_number_and_all()
    {
        Assert.Equal(CliOptions.DefaultTail, Parsed(Check("--bootstrap", "x:1")).Tail);
        Assert.Equal(0L, Parsed(Check("--bootstrap", "x:1", "--tail", "0")).Tail);
        Assert.Equal(long.MaxValue, Parsed(Check("--bootstrap", "x:1", "--tail", "all")).Tail);
        Assert.Contains("--tail", ParseError(Check("--bootstrap", "x:1", "--tail", "-5")), StringComparison.Ordinal);
    }

    [Fact]
    public void Needs_bootstrap_or_source_env() =>
        Assert.Contains("--bootstrap или --source-env", ParseError(Check()), StringComparison.Ordinal);

    [Fact]
    public void Bootstrap_and_source_env_together_are_not_allowed() =>
        Assert.Contains("вместе нельзя", ParseError(Check("--bootstrap", "x:1", "--source-env", "REPLAY_SOURCE")), StringComparison.Ordinal);

    [Fact]
    public void Topic_is_required() =>
        Assert.Contains("--topic", ParseError(["check", "--from", "2026-10-14 15:00:00+03:00", "--to", "2026-10-14 16:00:00+03:00", "--bootstrap", "x:1"]), StringComparison.Ordinal);

    [Fact]
    public void Time_without_offset_is_rejected() =>
        Assert.Contains("--from", ParseError(["check", "--topic", "t", "--from", "2026-10-14 15:00:00", "--to", "2026-10-14 16:00:00+03:00", "--bootstrap", "x:1"]), StringComparison.Ordinal);

    [Fact]
    public void From_must_be_before_to() =>
        Assert.Contains("раньше", ParseError(["check", "--topic", "t", "--from", "2026-10-14 16:00:00+03:00", "--to", "2026-10-14 15:00:00+03:00", "--bootstrap", "x:1"]), StringComparison.Ordinal);

    // ошибки самого разбора (неизвестный параметр, нет значения, повтор) — от System.CommandLine,
    // текст зависит от языка системы, поэтому проверяем только, что назван сам параметр

    [Fact]
    public void Unknown_parameter_is_rejected() =>
        Assert.Contains("--verbose", ParseError(Check("--bootstrap", "x:1", "--verbose", "yes")), StringComparison.Ordinal);

    [Fact]
    public void Missing_value_is_rejected() =>
        Assert.Contains("--out", ParseError(Check("--bootstrap", "x:1", "--out")), StringComparison.Ordinal);

    [Fact]
    public void Same_parameter_twice_is_rejected() =>
        Assert.Contains("--bootstrap", ParseError(Check("--bootstrap", "x:1", "--bootstrap", "y:2")), StringComparison.Ordinal);

    [Fact]
    public void Command_is_required() =>
        Assert.False(new ReplayCli(Now).TryParse([], out _, out _));

    // --- check: план отправки ---

    [Fact]
    public void Routes_with_target_are_parsed()
    {
        var o = Parsed(Check("--bootstrap", "x:1", "--routes", "routes.json", "--target-bootstrap", "y:1", "--partitioner", "murmur2"));
        Assert.Equal("routes.json", o.Routes);
        Assert.Equal("y:1", o.TargetBootstrap);
        Assert.Equal(PartitionStrategy.Murmur2, o.Partitioner);
    }

    [Fact]
    public void Default_partitioner_is_crc32()
    {
        var o = Parsed(Check("--bootstrap", "x:1", "--routes", "r.json", "--target-env", "REPLAY_TARGET"));
        Assert.Equal(PartitionStrategy.Crc32, o.Partitioner);
        Assert.Equal("REPLAY_TARGET", o.TargetEnv);
    }

    [Fact]
    public void Routes_need_target() =>
        Assert.Contains("нужна цель", ParseError(Check("--bootstrap", "x:1", "--routes", "r.json")), StringComparison.Ordinal);

    [Fact]
    public void Target_without_routes_is_rejected() =>
        Assert.Contains("без --routes", ParseError(Check("--bootstrap", "x:1", "--target-bootstrap", "y:1")), StringComparison.Ordinal);

    [Fact]
    public void Partitioner_without_routes_is_rejected() =>
        Assert.Contains("только вместе с --routes", ParseError(Check("--bootstrap", "x:1", "--partitioner", "keep")), StringComparison.Ordinal);

    [Fact]
    public void Unknown_partitioner_is_rejected() =>
        Assert.Contains("crc32, murmur2, keep", ParseError(Check("--bootstrap", "x:1", "--routes", "r.json", "--target-bootstrap", "y:1", "--partitioner", "random")), StringComparison.Ordinal);

    // --- execute ---

    [Fact]
    public void Execute_is_parsed()
    {
        var o = Parsed(["execute", "--plan", "/tmp/run-1/plan.json", "--target-bootstrap", "y:1"]);
        Assert.Equal(RunMode.Execute, o.Mode);
        Assert.Equal("/tmp/run-1/plan.json", o.Plan);
        Assert.Equal("y:1", o.TargetBootstrap);
        Assert.Equal(Path.GetDirectoryName(Path.GetFullPath("/tmp/run-1/plan.json")), o.OutDir);
    }

    [Fact]
    public void Execute_needs_plan() =>
        Assert.Contains("--plan", ParseError(["execute", "--target-bootstrap", "y:1"]), StringComparison.Ordinal);

    [Fact]
    public void Execute_needs_target() =>
        Assert.Contains("укажи цель", ParseError(["execute", "--plan", "p.json"]), StringComparison.Ordinal);

    [Fact]
    public void Execute_targets_together_are_not_allowed() =>
        Assert.Contains("вместе нельзя", ParseError(["execute", "--plan", "p.json", "--target-bootstrap", "y:1", "--target-env", "T"]), StringComparison.Ordinal);

    [Fact]
    public void Execute_does_not_take_source_parameters() =>
        Assert.Contains("--topic", ParseError(["execute", "--plan", "p.json", "--target-bootstrap", "y:1", "--topic", "orders"]), StringComparison.Ordinal);
}
