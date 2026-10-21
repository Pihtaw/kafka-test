namespace Samples.Contracts;

public class OrderCreated
{
    public required long OrderId { get; init; }
    public required decimal Amount { get; init; }
    public string? Currency { get; init; }
    public IReadOnlyList<OrderLine> Lines { get; init; } = [];
}

// вложенный тип: используется внутри OrderCreated, сам событием не является
public class OrderLine
{
    public string Product { get; init; } = "";
    public int Quantity { get; init; }
}

public class OrderCancelled
{
    public required long OrderId { get; init; }
    public required string Reason { get; init; }
}

public class PaymentCaptured
{
    public required long PaymentId { get; init; }
}

// только константы, не реестр
public static class Topics
{
    public const string Orders = "orders";
}
