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

        // другой формат даёт понятный отказ, а не ошибку парсера JSON.
        if (!HeaderReader.TryReadOptional(record.Headers, HeaderNames.ContentType, out var contentType, out fail))
            return fail!;
        if (!HeaderReader.TryReadOptional(record.Headers, HeaderNames.ContentEncoding, out var contentEncoding, out fail))
            return fail!;
        if (FormatProblem(contentType, contentEncoding) is { } problem)
            return new Decision(Verdict.ConversionError, Reason.UnsupportedFormat, problem);

        object? result;
        try
        {
            // Task.Run: модуль может работать синхронно (JsonModule десериализует до того, как вернуть Task),
            // и тогда WaitAsync начал бы отсчёт уже после работы. Так таймаут ограничивает и синхронную часть;
            // сам поток после таймаута не прервать — он доработает в фоне, но результат не ждём
            // один источник времени: тот же токен получают модуль, Task.Run и ожидание
            using var cts = new CancellationTokenSource(timeout);
            result = await Task.Run(() => converter.ConvertAsync(record.Payload, dtoType, cts.Token), cts.Token).WaitAsync(cts.Token).ConfigureAwait(false);
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

    // "application/json; charset=utf-8" — тоже JSON. Другой формат появится — здесь будет выбор модуля, а не отказ
    public static string? FormatProblem(string? contentType, string? contentEncoding)
    {
        string? mediaType = contentType?.Split(';')[0].Trim();
        if (mediaType is not null && !string.Equals(mediaType, "application/json", StringComparison.OrdinalIgnoreCase))
            return $"{HeaderNames.ContentType}: {contentType} — поддерживается только application/json";

        string? encoding = contentEncoding?.Trim();
        if (encoding is not null && !string.Equals(encoding, "utf-8", StringComparison.OrdinalIgnoreCase)
                                 && !string.Equals(encoding, "utf8", StringComparison.OrdinalIgnoreCase))
            return $"{HeaderNames.ContentEncoding}: {contentEncoding} — поддерживается только utf-8";

        return null;
    }
}
