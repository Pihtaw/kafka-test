using System.Text.Json;

namespace MessageCheck;

// проверки: 1) заголовки → 2) каталог → 3) модуль преобразования → 4) проверка ответа → решение.
public class Processor(EventCatalog catalog, IConverter converter, TimeSpan timeout)
{
    public async Task<Decision> ProcessAsync(KafkaRecord record)
    {
        if (!record.Headers.TryGetValue("eventType", out var eventType) || string.IsNullOrWhiteSpace(eventType))
            return new Decision(Verdict.Rejected, Reason.MissingEventType, "нет заголовка eventType");

        if (!record.Headers.TryGetValue("version", out var version) || string.IsNullOrWhiteSpace(version))
            return new Decision(Verdict.Rejected, Reason.MissingVersion, "нет заголовка version");

        if (!catalog.TryGet(eventType, version, out var dtoType))
            return new Decision(Verdict.Rejected, Reason.UnknownEvent, $"{eventType} v{version} нет в каталоге");

        object? result;
        try
        {
            using var cts = new CancellationTokenSource(timeout);
            result = await converter.ConvertAsync(record.Payload, dtoType, cts.Token).WaitAsync(timeout);
        }
        catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
        {
            // модуль не успел.
            return new Decision(Verdict.ConversionError, Reason.ModuleTimeout, $"модуль не ответил за {timeout.TotalMilliseconds} мс");
        }
        catch (JsonException ex)
        {
            // плохие данные, модуль хорошо
            return new Decision(Verdict.ConversionError, Reason.InvalidPayload, ex.Message);
        }
        catch (Exception ex)
        {
            // иначе
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
