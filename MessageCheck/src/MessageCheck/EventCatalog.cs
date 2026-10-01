namespace MessageCheck;

// словарь по паре (eventType, version) говорит в какой DTO превращать
public class EventCatalog
{
    private readonly Dictionary<(string EventType, string Version), Type> _types = new();

    // Добавить запись в каталог
    public EventCatalog Add<TDto>(string eventType, string version)
    {
        _types[(eventType, version)] = typeof(TDto);
        return this;
    }

    public bool TryGet(string eventType, string version, out Type dtoType)
        => _types.TryGetValue((eventType, version), out dtoType!);

    public static EventCatalog Default() => new EventCatalog()
        .Add<OrderCreatedV1>("OrderCreated", "1")
        .Add<OrderCreatedV2>("OrderCreated", "2")
        .Add<OrderCancelledV1>("OrderCancelled", "1");
}
