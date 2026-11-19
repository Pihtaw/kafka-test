using KafkaReplay;

namespace KafkaReplay.Tests;

// запись через временный файл и переименование: файл либо старый целиком, либо новый целиком
public sealed class AtomicFileTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("replay-atomic-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [Fact]
    public void Writes_new_file_and_leaves_no_tmp()
    {
        string path = Path.Combine(_dir, "execution.json");
        AtomicFile.WriteAllText(path, "{\"a\":1}");

        Assert.Equal("{\"a\":1}", File.ReadAllText(path));
        Assert.False(File.Exists(path + ".tmp"));
    }

    [Fact]
    public async Task Replaces_existing_file()
    {
        string path = Path.Combine(_dir, "plan.json");
        await File.WriteAllTextAsync(path, "старое");
        await AtomicFile.WriteAllTextAsync(path, "новое");

        Assert.Equal("новое", await File.ReadAllTextAsync(path));
        Assert.Single(Directory.GetFiles(_dir));
    }

    [Fact]
    public void Leftover_tmp_from_interrupted_write_does_not_break_next_write()
    {
        string path = Path.Combine(_dir, "execution.json");
        File.WriteAllText(path, "{\"целый\":true}");
        File.WriteAllText(path + ".tmp", "{\"обор");      // как будто прошлую запись прервали посередине

        Assert.Equal("{\"целый\":true}", File.ReadAllText(path));   // настоящий файл не тронут
        AtomicFile.WriteAllText(path, "{\"новый\":true}");
        Assert.Equal("{\"новый\":true}", File.ReadAllText(path));
        Assert.False(File.Exists(path + ".tmp"));
    }
}
