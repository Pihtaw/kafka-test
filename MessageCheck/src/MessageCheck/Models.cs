namespace MessageCheck;

public record Header(string Key, byte[]? Value);

// в байтах
public record KafkaRecord(
    IReadOnlyList<Header> Headers,
    byte[]? Key,
    byte[]? Payload);

public enum Verdict
{
    Allowed,
    Rejected,
    ConversionError,
    Skipped // чужое событие не битое
}

public enum Reason
{
    Ok,
    MissingEventType,
    MissingVersion,
    EmptyHeader,
    InvalidHeaderEncoding, // не UTF-8
    DuplicateHeader,       // заголовок повторяется с разными значениями
    NotOurEvent,           // чужое событие
    UnknownVersion,
    InvalidPayload,
    ModuleTimeout,
    ModuleCrashed,
    ModuleInvalidResponse,
    Tombstone
}

public record Decision(
    Verdict Verdict,
    Reason Reason,
    string? Details = null,
    string? ResultJson = null)
{
    public bool CanSend => Verdict == Verdict.Allowed;
}
