using System.Text.Json;

namespace MessageCheck;

// проверка на чужое событие
public class Processor(EventCatalog catalog, IConverter converter, TimeSpan timeout)
{
    public async Task<Decision> ProcessAsync(KafkaRecord record)
    {
        if (record.Payload is null)
            return new Decision(Verdict.Skipped, Reason.Tombstone, "value = null: tombstone, не событие");

        // UTF-8 без повторов
        if (!HeaderReader.TryRead(record.Headers, "eventType", Reason.MissingEventType, out var eventType, out var fail))
            return fail!;

        // чужое событие
        if (!catalog.Knows(eventType))
            return new Decision(Verdict.Skipped, Reason.NotOurEvent, $"{eventType} — чужой тип");

        if (!HeaderReader.TryRead(record.Headers, "version", Reason.MissingVersion, out var version, out fail))
            return fail!;

        if (!catalog.TryGet(eventType, version, out var dtoType))
            return new Decision(Verdict.Rejected, Reason.UnknownVersion, $"{eventType} v{version} нет в каталоге");

        object? result;
        try
        {
            // TODO: ConvertAsync сейчас синхронный, проверить на настоящем
            using var cts = new CancellationTokenSource(timeout);
            result = await converter.ConvertAsync(record.Payload, dtoType, cts.Token).WaitAsync(timeout);
        }
        catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
        {
            return new Decision(Verdict.ConversionError, Reason.ModuleTimeout, $"модуль не ответил за {timeout.TotalMilliseconds} мс");
        }
        catch (JsonException ex)
        {
            return new Decision(Verdict.ConversionError, Reason.InvalidPayload, ex.Message);
        }
        catch (Exception ex)
        {
            return new Decision(Verdict.ConversionError, Reason.ModuleCrashed, $"{ex.GetType().Name}: {ex.Message}");
        }

        if (result is null)
            return new Decision(Verdict.ConversionError, Reason.ModuleInvalidResponse, "модуль вернул null");

        if (result.GetType() != dtoType)
            return new Decision(Verdict.ConversionError, Reason.ModuleInvalidResponse,
                $"ожидали {dtoType.Name}, модуль вернул {result.GetType().Name}");

        return new Decision(Verdict.Allowed, Reason.Ok, $"{eventType} v{version} → {dtoType.Name}", JsonModule.ToJson(result));
    }
}
