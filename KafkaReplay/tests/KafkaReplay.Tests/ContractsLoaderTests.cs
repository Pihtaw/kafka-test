using System.Runtime.Loader;
using System.Text;
using KafkaReplay;
using MessageCheck;

namespace KafkaReplay.Tests;

// загрузка контрактов из сборки реестра
public class ContractsLoaderTests
{
    private static string RegistryPath => Path.Combine(AppContext.BaseDirectory, "SampleRegistry.dll");

    private static LoadedContracts Load()
    {
        Assert.True(ContractsLoader.TryLoad(RegistryPath, out var contracts, out var error), error);
        return contracts!;
    }

    private static string LoadError(string path)
    {
        Assert.False(ContractsLoader.TryLoad(path, out var contracts, out var error));
        Assert.Null(contracts);
        return error!;
    }

    [Fact]
    public void Event_types_come_from_topics()
    {
        var c = Load();

        Assert.Equal(3, c.Catalog.Count);
        Assert.True(c.Catalog.TryGet("Samples.Contracts.OrderCreated", out _));
        Assert.True(c.Catalog.TryGet("Samples.Contracts.OrderCancelled", out _));
        Assert.True(c.Catalog.TryGet("Samples.Contracts.PaymentCaptured", out _));
    }

    [Fact]
    public void Nested_type_is_not_an_event() =>
        // OrderLine есть в сборке, но в реестре не объявлен: это часть OrderCreated, а не событие
        Assert.False(Load().Catalog.TryGet("Samples.Contracts.OrderLine", out _));

    [Fact]
    public void Topics_know_their_event_types()
    {
        var c = Load();

        Assert.True(c.HasTopic("orders"));
        Assert.True(c.HasTopic("payments"));
        Assert.False(c.HasTopic("comment"));
        Assert.True(c.IsDeclaredFor("orders", "Samples.Contracts.OrderCreated"));
        Assert.False(c.IsDeclaredFor("payments", "Samples.Contracts.OrderCreated"));
    }

    [Fact]
    public void Versions_are_taken_from_assemblies()
    {
        var c = Load();

        Assert.Equal("2.3.0", c.RegistryVersion);
        Assert.Equal("2.3.0", c.VersionOf("Samples.Contracts.OrderCreated"));
    }

    [Fact]
    public void Contracts_are_loaded_in_their_own_context()
    {
        Assert.True(Load().Catalog.TryGet("Samples.Contracts.OrderCreated", out var type));
        Assert.NotEqual(AssemblyLoadContext.Default, AssemblyLoadContext.GetLoadContext(type.Assembly));
    }

    [Fact]
    public async Task Event_is_read_with_loaded_contract()
    {
        var processor = new Processor(Load().Catalog, new JsonModule(), TimeSpan.FromSeconds(1));
        var record = new KafkaRecord(
            [new Header(HeaderNames.EventType, "Samples.Contracts.OrderCreated"u8.ToArray())],
            "1"u8.ToArray(),
            Encoding.UTF8.GetBytes("""{"OrderId":1,"Amount":500,"Lines":[{"Product":"pizza","Quantity":2}]}"""));

        var decision = await processor.ProcessAsync(record);

        Assert.Equal(Verdict.Allowed, decision.Verdict);
        Assert.Contains("\"Product\":\"pizza\"", decision.ResultJson);
    }

    [Fact]
    public void Missing_file_is_reported() =>
        Assert.Contains("нет файла", LoadError(Path.Combine(AppContext.BaseDirectory, "no-such-registry.dll")));

    [Fact]
    public void Not_a_dotnet_assembly_is_reported()
    {
        string path = Path.Combine(Path.GetTempPath(), $"not-an-assembly-{Guid.NewGuid():N}.dll");
        File.WriteAllText(path, "просто текст");
        try { Assert.Contains("не сборка .NET", LoadError(path)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void Assembly_without_topics_is_reported() =>
        Assert.Contains("нет класса Topics", LoadError(typeof(EventCatalog).Assembly.Location));
}
