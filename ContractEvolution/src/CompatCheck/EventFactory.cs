using System.Reflection;
using System.Text.Json;
using Contracts;

namespace CompatCheck;

// одно сгенерированное событие: какой версией контракта его отправили, что за случай и байты JSON
public sealed record GeneratedEvent(int Version, string Case, byte[] Payload);

// Генератор событий: для версии контракта создаёт набор событий —
// одно норм и по одному на каждое граничное значение каждого поля.
public static class EventFactory
{
    public static readonly JsonSerializerOptions ProducerOptions = new(JsonSerializerDefaults.Web);

    // обычное значение поля
    private static object? Typical(Type t) =>
        t == typeof(string) ? "A-101"
        : t == typeof(int) ? 42
        : t == typeof(long) ? 42L
        : t == typeof(double) ? 42.0
        : t == typeof(decimal) ? 42m
        : t == typeof(bool) ? true
        : throw new NotSupportedException($"нет значений для типа {t.Name}");

    // граничные значения: ломаем старые версии
    private static (string Label, object? Value)[] Boundaries(Type t) =>
        t == typeof(string) ? [("\"\"", ""), ("null", null)]
        : t == typeof(int) ? [("0", 0), ("int.MaxValue", int.MaxValue), ("int.MinValue", int.MinValue)]
        : t == typeof(long) ? [("int.MaxValue + 1", (long)int.MaxValue + 1), ("int.MinValue − 1", (long)int.MinValue - 1), ("long.MaxValue", long.MaxValue)]
        : t == typeof(double) ? [("1.5", 1.5), ("2.0", 2.0), ("1e20", 1e20)]
        : t == typeof(decimal) ? [("1.5m", 1.5m)]
        : t == typeof(bool) ? [("false", false)]
        : [];

    public static List<GeneratedEvent> For(ContractVersion version)
    {
        var props = version.Type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var events = new List<GeneratedEvent> { Make(version, "обычное", props, null, null) };

        foreach (var p in props)
            foreach (var (label, value) in Boundaries(p.PropertyType))
                events.Add(Make(version, $"{p.Name} = {label}", props, p, value));

        return events;
    }

    public static List<GeneratedEvent> ForAll() => OrderCreatedVersions.All.SelectMany(For).ToList();

    private static GeneratedEvent Make(ContractVersion v, string name, PropertyInfo[] props, PropertyInfo? special, object? value)
    {
        var dto = Activator.CreateInstance(v.Type)!;
        foreach (var p in props)
            p.SetValue(dto, p == special ? value : Typical(p.PropertyType));
        return new GeneratedEvent(v.Version, name, JsonSerializer.SerializeToUtf8Bytes(dto, v.Type, ProducerOptions));
    }
}
