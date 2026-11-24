using KafkaReplay;
using MessageCheck;

namespace KafkaReplay.Tests;

// replay schemas на учебном реестре: схема на каждый тип, и check --schemas их находит
public sealed class SchemasCommandTests : IDisposable
{
    private static string RegistryPath => Path.Combine(AppContext.BaseDirectory, "SampleRegistry.dll");
    private readonly string _dir = Directory.CreateTempSubdirectory("replay-schemas-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [Fact]
    public void Writes_one_schema_per_event_type_and_check_can_load_them()
    {
        Assert.Equal(ExitCodes.Ok, SchemasCommand.Run(RegistryPath, _dir));
        Assert.Equal(3, Directory.GetFiles(_dir, "*.json").Length);   // OrderCreated, OrderCancelled, PaymentCaptured

        Assert.True(SchemaSet.TryLoad(_dir, out var schemas, out var error), error);
        Assert.NotNull(schemas!.Find("Samples.Contracts.OrderCreated"));

        // правильное событие проходит; null в не-nullable Reason — ⚠
        Assert.Empty(schemas.Check("Samples.Contracts.OrderCreated", """{"OrderId":1,"Amount":500,"Currency":null,"Lines":[{"Product":"pizza","Quantity":2}]}"""u8.ToArray())!);
        var v = Assert.Single(schemas.Check("Samples.Contracts.OrderCancelled", """{"OrderId":1,"Reason":null}"""u8.ToArray())!);
        Assert.Equal("$.Reason", v.Path);
        Assert.True(v.IsWarning);
    }

    [Fact]
    public void Regeneration_removes_own_stale_schemas_and_keeps_official_ones()
    {
        string stale = Path.Combine(_dir, "Samples.Contracts.Removed.json");      // тип, которого больше нет в реестре
        string official = Path.Combine(_dir, "ClickToEatForecastEvent.json");      // положен руками, без метки
        File.WriteAllText(stale, $$$"""{"$comment":"{{{SchemasCommand.GeneratedMark}}} из SampleRegistry.dll 1.0.0","type":"object"}""");
        File.WriteAllText(official, """{"title":"ClickToEatForecastEvent","type":"object"}""");

        Assert.Equal(ExitCodes.Ok, SchemasCommand.Run(RegistryPath, _dir));

        Assert.False(File.Exists(stale));
        Assert.True(File.Exists(official));
        Assert.Contains(SchemasCommand.GeneratedMark, File.ReadAllText(Path.Combine(_dir, "Samples.Contracts.OrderCreated.json")), StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_registry_is_refused() =>
        Assert.Equal(ExitCodes.Blocked, SchemasCommand.Run(Path.Combine(_dir, "nope.dll"), _dir));
}
