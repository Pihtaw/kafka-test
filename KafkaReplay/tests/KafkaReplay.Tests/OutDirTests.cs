using KafkaReplay;

namespace KafkaReplay.Tests;

// dry-run не пишет в папку прошлого запуска: старый plan.json можно было бы принять за новый и отправить
public sealed class OutDirTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("replay-out-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [Fact]
    public void Empty_or_missing_folder_is_fine()
    {
        Assert.Null(DryRunCommand.LeftoversProblem(_dir));
        Assert.Null(DryRunCommand.LeftoversProblem(Path.Combine(_dir, "new")));
    }

    [Fact]
    public void Old_report_alone_is_fine()
    {
        File.WriteAllText(Path.Combine(_dir, "report.md"), "старый отчёт");
        Assert.Null(DryRunCommand.LeftoversProblem(_dir));
    }

    [Theory]
    [InlineData("plan.json")]
    [InlineData("execution.json")]
    public void Old_plan_or_execution_blocks(string file)
    {
        File.WriteAllText(Path.Combine(_dir, file), "{}");
        Assert.Contains(file, DryRunCommand.LeftoversProblem(_dir)!, StringComparison.Ordinal);
    }
}
