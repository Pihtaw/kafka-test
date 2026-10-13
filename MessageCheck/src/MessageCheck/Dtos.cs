namespace MessageCheck.SampleContracts;

// тип НЕ создаётся заново на каждую версию
// а меняется от версии пакета к версии пакета
// в заголовке Event-Type приходит полное имя класса
// required — поле обязано прийти в JSON, иначе ошибка

public record OrderCreated
{
    public required long OrderId { get; init; }
    public required decimal Amount { get; init; }

    // необязательное: старые события без него должны читаться
    public string? Currency { get; init; }
}

public record OrderCancelled
{
    public required long OrderId { get; init; }
    public required string Reason { get; init; }
}
