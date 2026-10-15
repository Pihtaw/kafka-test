using Samples.Contracts;

namespace Samples.Registry;

public record Topic(string Name, IReadOnlyCollection<Type> EventTypes);

// Топики и типы событий в них — как Topics в event-registry
public static class Topics
{
    public static readonly Topic Orders = new("orders", [typeof(OrderCreated), typeof(OrderCancelled)]);
    public static readonly Topic Payments = new("payments", [typeof(PaymentCaptured)]);

    // не топик: у строки нет Name и EventTypes — загрузчик должен пропустить
    public static readonly string Comment = "не топик";
}
