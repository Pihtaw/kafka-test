using MessageCheck.SampleContracts;

namespace MessageCheck;

// Каталог: полное имя типа из заголовка Event-Type -> класс
public class EventCatalog
{
    // с учётом регистра, как Dodo.Kafka: "Dodo.X.OrderCreated" и "dodo.x.ordercreated" — разные типы
    private readonly Dictionary<string, Type> _types = new(StringComparer.Ordinal);

    public EventCatalog Add<TDto>() => Add(typeof(TDto));

    public EventCatalog Add(Type dtoType)
    {
        _types[dtoType.FullName!] = dtoType;
        return this;
    }

    public int Count => _types.Count;

    // все типы событий каталога: полное имя (как в Event-Type) → класс
    public IReadOnlyDictionary<string, Type> Types => _types;

    // чужое событие
    public bool TryGet(string eventType, out Type dtoType) => _types.TryGetValue(eventType, out dtoType!);

    public static EventCatalog Default() => new EventCatalog()
        .Add<OrderCreated>()
        .Add<OrderCancelled>();
}
