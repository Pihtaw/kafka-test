using System.Globalization;

namespace KafkaReplay;

public record CliOptions(
    string Topic,
    DateTimeOffset From,
    DateTimeOffset To,
    string? Bootstrap,   // локальная Kafka без входа
    string? SourceEnv,   // или имя переменной окружения со строкой подключения
    string OutDir)
{
    public const string TimeFormat = "yyyy-MM-dd HH:mm:sszzz";

    public const string Usage = """
        replay --topic <топик> --from "<с>" --to "<по>" (--bootstrap <host:port> | --source-env <ПЕРЕМЕННАЯ>) [--out <папка>]

          --topic       топик-источник
          --from, --to  окно по времени в формате yyyy-MM-dd HH:mm:sszzz, например "2026-10-14 15:00:00+03:00"
          --bootstrap   локальная Kafka без входа, например localhost:9092
          --source-env  имя переменной окружения, где лежит строка подключения в формате Dodo.Kafka:
                        BootstrapServers=...;Type=EventHubSas;SharedAccessKeyName=...;SharedAccessKey=...
          --out         папка для отчёта, по умолчанию run-<дата>-<время>
        """;

    private static readonly string[] Known = ["--topic", "--from", "--to", "--bootstrap", "--source-env", "--out"];

    public static bool TryParse(string[] args, DateTimeOffset now, out CliOptions? options, out string? error)
    {
        options = null;

        var values = new Dictionary<string, string>();
        for (int i = 0; i < args.Length; i++)
        {
            string name = args[i];
            if (!Known.Contains(name)) { error = $"неизвестный параметр {name}"; return false; }
            if (i + 1 >= args.Length || args[i + 1].StartsWith("--")) { error = $"нет значения у {name}"; return false; }
            if (!values.TryAdd(name, args[i + 1])) { error = $"{name} указан дважды"; return false; }
            i++;
        }

        if (!values.TryGetValue("--topic", out var topic) || string.IsNullOrWhiteSpace(topic)) { error = "не указан --topic"; return false; }
        if (!values.TryGetValue("--from", out var fromText)) { error = "не указан --from"; return false; }
        if (!values.TryGetValue("--to", out var toText)) { error = "не указан --to"; return false; }
        if (!TryParseTime(fromText, out var from)) { error = $"--from: время должно быть в формате {TimeFormat}, например 2026-10-14 15:00:00+03:00"; return false; }
        if (!TryParseTime(toText, out var to)) { error = $"--to: время должно быть в формате {TimeFormat}, например 2026-10-14 15:00:00+03:00"; return false; }
        if (from >= to) { error = "«с» должно быть раньше «по»"; return false; }

        values.TryGetValue("--bootstrap", out var bootstrap);
        values.TryGetValue("--source-env", out var sourceEnv);
        if (bootstrap is null && sourceEnv is null) { error = "укажи --bootstrap или --source-env"; return false; }
        if (bootstrap is not null && sourceEnv is not null) { error = "--bootstrap и --source-env вместе нельзя, выбери один"; return false; }

        string outDir = values.TryGetValue("--out", out var o) ? o : $"run-{now:yyyyMMdd-HHmmss}";

        options = new CliOptions(topic, from, to, bootstrap, sourceEnv, outDir);
        error = null;
        return true;
    }

    private static bool TryParseTime(string s, out DateTimeOffset t) =>
        DateTimeOffset.TryParseExact(s, TimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out t);
}
