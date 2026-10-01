namespace MessageCheck;

// Одно сообщение «как из Кафки», но без Кафки: просто данные в памяти.
// В настоящей Кафке заголовки и payload — байты; здесь для простоты строки.
public record KafkaRecord(
    Dictionary<string, string> Headers,
    string? Key,
    string Payload);

public enum Verdict
{
    Allowed,
    Rejected,
    ConversionError  // ошибка на этапе модуля преобразования, тл
}

public enum Reason
{
    Ok,
    MissingEventType,      // нет заголовка eventType
    MissingVersion,        // нет заголовка version
    UnknownEvent,          // пары (eventType, version) нет в каталоге
    InvalidPayload,        // JSON битый или не подходит под DTO
    ModuleTimeout,         // тл модуля
    ModuleCrashed,         // модуль упал с неожиданной ошибкой
    ModuleInvalidResponse  // модуль ответил, но нулом или не того типа
}

public record Decision(
    Verdict Verdict,
    Reason Reason,
    string? Details = null,
    string? ResultJson = null)
{
    public bool CanSend => Verdict == Verdict.Allowed;
}
