namespace MessageCheck;

public class EventCatalog
{
    private readonly Dictionary<(string EventType, string Version), Type> _types = new();

    public EventCatalog Add<TDto>(string eventType, string version)
    {
        _types[(eventType, version)] = typeof(TDto);
        return this;
    }

    public bool TryGet(string eventType, string version, out Type dtoType)
        => _types.TryGetValue((eventType, version), out dtoType!);

    // чужое событие
    public bool Knows(string eventType) => _types.Keys.Any(k => k.EventType == eventType);

    public static EventCatalog Default() => new EventCatalog()
        .Add<OrderCreatedV1>("OrderCreated", "1")
        .Add<OrderCreatedV2>("OrderCreated", "2")
        .Add<OrderCancelledV1>("OrderCancelled", "1");
}
