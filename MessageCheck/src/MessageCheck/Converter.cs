using System.Text.Json;
using System.Text.Json.Serialization;

namespace MessageCheck;

public interface IConverter
{
    // байты в dtoType, ct при таймауте
    Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct);
}

public class JsonModule : IConverter
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow // лишнее поле в JSON - ошибка
    };

    public Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct)
    {
        // JsonException на битом JSON
        var result = JsonSerializer.Deserialize(payload, dtoType, Options);
        return Task.FromResult(result);
    }

    public static string ToJson(object dto) => JsonSerializer.Serialize(dto, dto.GetType(), Options);
}
