using System.Text.Json;
using System.Text.Json.Nodes;
using Contracts;
using MessageCheck;

namespace CompatCheck;

public enum Outcome
{
    Ok,             // прочиталось, все поля совпали
    ExtraIgnored,   // прочиталось; новых полей старый контракт не видит
    SilentDefault,  // прочиталось БЕЗ ошибки, но поле старого контракта не пришло и стало default (0, "", null)
    ValueChanged,   // прочиталось БЕЗ ошибки, но значение поля изменилось
    Exploded        // упали
}

public sealed record CheckResult(int Producer, int Consumer, string Case, Outcome Outcome, string Details);

public sealed record ConsumerMode(string Name, IConverter Converter);

public sealed class LenientJsonModule : IConverter
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct) =>
        Task.FromResult(JsonSerializer.Deserialize(payload, dtoType, Options));
}

public static class Compat
{
    public static readonly ConsumerMode Lenient = new("System.Text.Json, Web-настройки (лишние поля игнорируются)", new LenientJsonModule());
    public static readonly ConsumerMode Strict = new("MessageCheck.JsonModule (лишние поля запрещены)", new JsonModule());

    // старый сервис читает событие, отправленное контрактом новее
    public static async Task<CheckResult> CheckAsync(GeneratedEvent e, ContractVersion consumer, ConsumerMode mode)
    {
        object? dto;
        try
        {
            dto = await mode.Converter.ConvertAsync(e.Payload, consumer.Type, CancellationToken.None);
        }
        catch (Exception ex)
        {
            return new(e.Version, consumer.Version, e.Case, Outcome.Exploded, $"{ex.GetType().Name}: {FirstLine(ex.Message)}");
        }
        if (dto is null)
            return new(e.Version, consumer.Version, e.Case, Outcome.Exploded, "получили null");

        // сравним отправленное и увиденное
        var sent = JsonNode.Parse(e.Payload)!.AsObject();
        var seen = JsonSerializer.SerializeToNode(dto, consumer.Type, EventFactory.ProducerOptions)!.AsObject();

        var problems = new List<(Outcome Outcome, string Text)>();
        foreach (var (field, seenValue) in seen)
        {
            if (!sent.TryGetPropertyValue(field, out var sentValue))
                problems.Add((Outcome.SilentDefault, $"{field} не пришло → {Show(seenValue)}"));
            else if (Show(sentValue) != Show(seenValue))
                problems.Add((Outcome.ValueChanged, $"{field}: {Show(sentValue)} → {Show(seenValue)}"));
        }
        foreach (var (field, _) in sent)
            if (!seen.ContainsKey(field))
                problems.Add((Outcome.ExtraIgnored, $"{field} не виден"));

        var worst = problems.Count == 0 ? Outcome.Ok : problems.Max(p => p.Outcome);
        return new(e.Version, consumer.Version, e.Case, worst, string.Join("; ", problems.Select(p => p.Text)));
    }

    public static async Task<List<CheckResult>> RunAsync(IEnumerable<GeneratedEvent> events, ConsumerMode mode)
    {
        var results = new List<CheckResult>();
        foreach (var e in events)
            foreach (var consumer in OrderCreatedVersions.All.Where(c => c.Version <= e.Version))
                results.Add(await CheckAsync(e, consumer, mode));
        return results;
    }

    private static string Show(JsonNode? n) => n is null ? "null" : n.ToJsonString();

    private static string FirstLine(string s) => s.Split('\n')[0].Split(" | LineNumber")[0].Trim();
}
