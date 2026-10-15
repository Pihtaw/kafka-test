using Confluent.Kafka;

namespace KafkaReplay;

// как в docs/connection-strings.md в kafka-tools
//   BootstrapServers=kafka.stand-name:9092;
//   BootstrapServers=ns.servicebus.windows.net:9093;Type=EventHubSas;SharedAccessKeyName=..;SharedAccessKey=..;
public sealed class ConnectionSettings
{
    public string Type { get; }
    public string BootstrapServers { get; }
    private readonly Dictionary<string, string> _fields;

    private ConnectionSettings(string type, string bootstrap, Dictionary<string, string> fields)
    {
        Type = type;
        BootstrapServers = bootstrap;
        _fields = fields;
    }

    // локальная Kafka без входа
    public static ConnectionSettings Plain(string bootstrap) =>
        new("plain", bootstrap, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));

    public static bool TryParse(string connectionString, out ConnectionSettings? settings, out string? error)
    {
        settings = null;
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int eq = part.IndexOf('=');
            if (eq <= 0) { error = "в строке подключения есть часть без вида Имя=значение"; return false; }

            string name = part[..eq].Trim();
            string value = part[(eq + 1)..].Trim();
            if (value.Length >= 2 && value[0] == '"' && value[^1] == '"') value = value[1..^1];

            if (!fields.TryAdd(name, value)) { error = $"в строке подключения поле {name} указано дважды"; return false; }
        }

        if (!fields.TryGetValue("BootstrapServers", out var bootstrap) || bootstrap.Length == 0)
        {
            error = "в строке подключения нет поля BootstrapServers";
            return false;
        }

        string type = fields.TryGetValue("Type", out var t) ? t.ToLowerInvariant() : "plain";

        if (type == "eventhubad")
        {
            // токен для AD берётся через DefaultAzureCredential (Azure.Identity).
            // TODO: добавить когда будет
            error = "Type=EventHubAd пока не поддерживается, используйте EventHubSas";
            return false;
        }

        // какие поля обязательны для каждого типа
        string[]? required = type switch
        {
            "plain" => [],
            "eventhubsas" => ["SharedAccessKeyName", "SharedAccessKey"],
            _ => null
        };
        if (required is null)
        {
            error = $"неизвестный Type={t}, есть: EventHubSas (или без Type для dev-стенда)";
            return false;
        }

        foreach (var name in required)
        {
            if (!fields.TryGetValue(name, out var v) || v.Length == 0)
            {
                error = $"для Type={t} в строке подключения нужно поле {name}";
                return false;
            }
        }

        settings = new ConnectionSettings(type, bootstrap, fields);
        error = null;
        return true;
    }

    // Настройки клиента Kafka kafka-tools
    public ClientConfig ToClientConfig()
    {
        var c = new ClientConfig
        {
            BootstrapServers = BootstrapServers,
            ClientId = "kafka-replay",          // чтобы на стороне брокера было видно, кто читает
            SocketKeepaliveEnable = true,       // Azure закрывает TCP-соединение, если оно простаивает дольше 240 с
            MetadataMaxAgeMs = 180_000          // kafka-tools - обновлять метаданные раньше, чем Azure закроет соединение
        };

        switch (Type)
        {
            case "eventhubsas":
                // Event Hubs
                c.SecurityProtocol = SecurityProtocol.SaslSsl;
                c.SaslMechanism = SaslMechanism.Plain;
                c.SaslUsername = "$ConnectionString";
                c.SaslPassword = $"Endpoint=sb://{Host}/;SharedAccessKeyName={_fields["SharedAccessKeyName"]};SharedAccessKey={_fields["SharedAccessKey"]}";
                break;
        }
        return c;
    }

    public string Describe() => $"{Type} {BootstrapServers}";

    private string Host => BootstrapServers.Split(',')[0].Trim().Split(':')[0];
}
