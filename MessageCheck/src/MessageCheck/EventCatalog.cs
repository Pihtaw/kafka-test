using MessageCheck.SampleContracts;

namespace MessageCheck;

// Каталог: полное имя типа из заголовка Event-Type -> класс
public class EventCatalog
{
    // без учёта регистра
    private readonly Dictionary<string, Type> _types = new(StringComparer.OrdinalIgnoreCase);

    public EventCatalog Add<TDto>()
    {
        _types[typeof(TDto).FullName!] = typeof(TDto);
        return this;
    }

    // чужое событие
    public bool TryGet(string eventType, out Type dtoType) => _types.TryGetValue(eventType, out dtoType!);

    public static EventCatalog Default() => new EventCatalog()
        .Add<OrderCreated>()
        .Add<OrderCancelled>();
}
