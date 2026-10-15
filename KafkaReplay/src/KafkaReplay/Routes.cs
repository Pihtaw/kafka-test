using System.Text.Json;

namespace KafkaReplay;

// Таблица маршрутизации: тип события (полное имя, как в Event-Type) -> целевой топик.
// Файл routes.json:
//   {
//     "MessageCheck.SampleContracts.OrderCreated": "orders-copy",
//     "MessageCheck.SampleContracts.OrderCancelled": "orders-copy",
//     "*": "everything-else"          ← необязательно: куда отправлять остальные наши события
//   }
// Нет маршрута — запись не отправляется
public sealed class Routes
{
    public const string Default = "*";
    private readonly Dictionary<string, string> _routes;

    private Routes(Dictionary<string, string> routes) => _routes = routes;

    public IReadOnlyCollection<string> TargetTopics => _routes.Values.Distinct(StringComparer.Ordinal).ToList();
    public IReadOnlyDictionary<string, string> All => _routes;

    public string? TargetFor(string eventType) =>
        _routes.TryGetValue(eventType, out var topic) ? topic : _routes.GetValueOrDefault(Default);

    public static Routes FromDictionary(IDictionary<string, string> routes) =>
        new(new Dictionary<string, string>(routes, StringComparer.OrdinalIgnoreCase));

    public static bool TryLoad(string path, out Routes? routes, out string? error)
    {
        routes = null;
        string json;
        try { json = File.ReadAllText(path); }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            error = $"не удалось прочитать таблицу маршрутизации {path}: {e.Message}";
            return false;
        }
        return TryParse(json, out routes, out error);
    }

    public static bool TryParse(string json, out Routes? routes, out string? error)
    {
        routes = null;
        Dictionary<string, string>? raw;
        try { raw = JsonSerializer.Deserialize<Dictionary<string, string>>(json); }
        catch (JsonException e)
        {
            error = $"таблица маршрутизации: ожидается JSON-объект «тип события → топик»: {e.Message}";
            return false;
        }
        if (raw is null || raw.Count == 0) { error = "таблица маршрутизации пустая"; return false; }

        foreach (var (type, topic) in raw)
        {
            if (string.IsNullOrWhiteSpace(type) || string.IsNullOrWhiteSpace(topic))
            {
                error = "в таблице маршрутизации пустой тип события или топик";
                return false;
            }
        }

        routes = FromDictionary(raw);
        error = null;
        return true;
    }

    // Запрет: нельзя отправлять в тот же топик того же кластера, откуда читали.
    public IReadOnlyList<string> RoutesBackToSource(string sourceTopic, string sourceBootstrap, string targetBootstrap)
    {
        if (!SameCluster(sourceBootstrap, targetBootstrap)) return [];
        return _routes.Where(r => string.Equals(r.Value, sourceTopic, StringComparison.Ordinal))
                      .Select(r => $"{r.Key} → {r.Value}").ToList();
    }

    public static bool SameCluster(string a, string b) => Normalize(a) == Normalize(b);

    private static string Normalize(string bootstrap) =>
        string.Join(",", bootstrap.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                                  .Select(s => s.ToLowerInvariant()).Order(StringComparer.Ordinal));
}
