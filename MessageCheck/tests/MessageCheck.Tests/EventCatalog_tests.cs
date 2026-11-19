using MessageCheck;

namespace MessageCheck.Tests;

// тип из Event-Type сравнивается побайтно, как в Dodo.Kafka: другой регистр — другой (чужой) тип
public class EventCatalogTests
{
    [Fact]
    public void Event_type_is_matched_exactly_including_case()
    {
        var catalog = EventCatalog.Default();
        Assert.True(catalog.TryGet("MessageCheck.SampleContracts.OrderCreated", out _));
        Assert.False(catalog.TryGet("messagecheck.samplecontracts.ordercreated", out _));
        Assert.False(catalog.TryGet("MessageCheck.SampleContracts.ORDERCREATED", out _));
    }
}
