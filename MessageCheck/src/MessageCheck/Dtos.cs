namespace MessageCheck;

// DTO — классы, в которые превращается JSON
// required — поле обязано быть в JSON - еще одна проверка на ошибку

public record OrderCreatedV1
{
    public required long OrderId { get; init; }
    public required decimal Amount { get; init; }
}

// версия 2 того же события: добавилось обязательное поле Currency.
public record OrderCreatedV2
{
    public required long OrderId { get; init; }
    public required decimal Amount { get; init; }
    public required string Currency { get; init; }
}

public record OrderCancelledV1
{
    public required long OrderId { get; init; }
    public required string Reason { get; init; }
}
