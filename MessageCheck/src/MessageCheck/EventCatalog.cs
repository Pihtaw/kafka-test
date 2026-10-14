namespace MessageCheck;

public class EventCatalog
{
    private readonly Dictionary<string, Dictionary<string, Type>> _types = new(StringComparer.OrdinalIgnoreCase);

    public EventCatalog Add<TDto>(string eventType, string version)
    {
        if (!_types.TryGetValue(eventType, out var versions))
            _types[eventType] = versions = new Dictionary<string, Type>();
        versions[version] = typeof(TDto);
        return this;
    }

    public bool TryGet(string eventType, string version, out Type dtoType)
    {
        dtoType = null!;
        return _types.TryGetValue(eventType, out var versions) && versions.TryGetValue(version, out dtoType!);
    }

    // чужое событие
    public bool Knows(string eventType) => _types.ContainsKey(eventType);

    public static EventCatalog Default() => new EventCatalog()
        .Add<OrderCreatedV1>("OrderCreated", "1")
        .Add<OrderCreatedV2>("OrderCreated", "2")
        .Add<OrderCancelledV1>("OrderCancelled", "1");
}
