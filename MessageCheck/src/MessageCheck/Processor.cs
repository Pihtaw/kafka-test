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
        if (!HeaderReader.TryRead(record.Headers, HeaderNames.EventType, Reason.MissingEventType, out var eventType, out var fail))
            return fail!;

        // чужое событие
        if (!catalog.TryGet(eventType, out var dtoType))
            return new Decision(Verdict.Skipped, Reason.NotOurEvent, $"{eventType} — чужой тип");

        // версия пакета — только для информации, тип по ней не выбираем
        if (!HeaderReader.TryReadOptional(record.Headers, HeaderNames.PackageVersion, out var version, out fail))
            return fail!;
        var versionText = version ?? "без версии";

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

        var resultJson = JsonModule.ToJson(result);
        var details = $"{eventType} ({versionText}) → {dtoType.Name}";

        // лишние поля не ошибка
        // их пропускают настоящие потребители, но показываем их
        var extra = JsonModule.ExtraFields(record.Payload, resultJson);
        if (extra.Count > 0)
            details += $"; лишние поля: {string.Join(", ", extra)}";

        return new Decision(Verdict.Allowed, Reason.Ok, details, resultJson);
    }
}
