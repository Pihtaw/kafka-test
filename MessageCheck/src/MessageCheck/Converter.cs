using System.Text.Json;
using System.Text.Json.Serialization;

namespace MessageCheck;

public interface IConverter
{
    // Превратить payload в объект типа dtoType.
    // ct — «кнопка отмены»: процессор нажмёт её, если модуль думает слишком долго.
    Task<object?> ConvertAsync(string payload, Type dtoType, CancellationToken ct);
}

public class JsonModule : IConverter
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow // лишнее поле в JSON - ошибка
    };

    public Task<object?> ConvertAsync(string payload, Type dtoType, CancellationToken ct)
    {
        // обработка если json битый
        var result = JsonSerializer.Deserialize(payload, dtoType, Options);
        return Task.FromResult(result);
    }

    public static string ToJson(object dto) => JsonSerializer.Serialize(dto, dto.GetType(), Options);
}
