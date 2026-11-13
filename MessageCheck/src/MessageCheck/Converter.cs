using System.Text.Encodings.Web;
using System.Text.Json;

namespace MessageCheck;

public interface IConverter
{
    // байты в dtoType, ct при таймауте
    Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct);
}

public class JsonModule : IConverter
{
    // лишние поля пропускаются; русские буквы в ToJson не превращаются в \uXXXX
    private static readonly JsonSerializerOptions Options = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    public Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct)
    {
        // JsonException на битом JSON
        var result = JsonSerializer.Deserialize(payload, dtoType, Options);
        return Task.FromResult(result);
    }

    public static string ToJson(object dto) => JsonSerializer.Serialize(dto, dto.GetType(), Options);

    // поля верхнего уровня, которые есть в событии, но которых нет в контракте (их пропустили)
    public static List<string> ExtraFields(byte[] payload, string resultJson)
    {
        using var sent = JsonDocument.Parse(payload);
        using var read = JsonDocument.Parse(resultJson);
        if (sent.RootElement.ValueKind != JsonValueKind.Object || read.RootElement.ValueKind != JsonValueKind.Object)
            return [];

        var known = read.RootElement.EnumerateObject().Select(p => p.Name).ToHashSet();
        return sent.RootElement.EnumerateObject().Select(p => p.Name).Where(n => !known.Contains(n)).ToList();
    }
}
