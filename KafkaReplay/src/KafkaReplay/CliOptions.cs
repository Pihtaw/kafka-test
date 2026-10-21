using System.Globalization;

namespace KafkaReplay;

public enum RunMode { DryRun, Execute }

//   без --execute: прочитать, проверить, написать отчет и план отправки
//   с --execute:   отправить по сохранённому плану, проверить побайтово, написать статусы
public record CliOptions(
    RunMode Mode,
    string? Topic,
    DateTimeOffset From,
    DateTimeOffset To,
    string? Bootstrap,
    string? SourceEnv,
    string? Contracts,          // путь к сборке реестра контрактов
    string? Routes,             // таблица маршрутизации - нет - план не строится
    string? TargetBootstrap,
    string? TargetEnv,
    PartitionStrategy Partitioner,
    string? Plan,               // для --execute путь к plan.json
    string OutDir)
{
    public const string TimeFormat = "yyyy-MM-dd HH:mm:sszzz";

    public const string Usage = """
        Проверка (ничего не отправляет):
          replay --topic <топик> --from "<с>" --to "<по>" (--bootstrap <host:port> | --source-env <ПЕРЕМЕННАЯ>)
                 [--contracts <Dodo.EventRegistry.dll>] [--out <папка>]
                 [--routes <routes.json> (--target-bootstrap <host:port> | --target-env <ПЕРЕМЕННАЯ>) [--partitioner crc32|murmur2|keep]]

        Отправка по плану:
          replay --execute --plan <папка>/plan.json (--target-bootstrap <host:port> | --target-env <ПЕРЕМЕННАЯ>)

          --topic            топик-источник
          --from, --to       окно по времени в формате yyyy-MM-dd HH:mm:sszzz, например "2026-10-14 15:00:00+03:00"
          --bootstrap        источник — локальная Kafka без входа, например localhost:9092
          --source-env       источник — имя переменной окружения со строкой подключения в формате Dodo.Kafka:
                             BootstrapServers=...;Type=EventHubSas;SharedAccessKeyName=...;SharedAccessKey=...
          --contracts        сборка реестра контрактов, которой проверять события (без него — встроенные учебные)
          --routes           таблица маршрутизации: тип события → целевой топик. С ней рядом с отчётом пишется plan.json
          --target-bootstrap цель — локальная Kafka без входа
          --target-env       цель — имя переменной окружения со строкой подключения
          --partitioner      как выбирать партицию в цели: crc32 (как .NET, по умолчанию), murmur2 (как Java), keep (как в источнике)
          --out              папка для отчёта и плана, по умолчанию run-<дата>-<время>
          --execute          отправить записи из plan.json; источник заново не читается и не проверяется
          --plan             путь к plan.json для --execute
        """;

    private static readonly string[] DryRunParams =
        ["--topic", "--from", "--to", "--bootstrap", "--source-env", "--contracts", "--routes", "--target-bootstrap", "--target-env", "--partitioner", "--out"];
    private static readonly string[] ExecuteParams = ["--plan", "--target-bootstrap", "--target-env"];

    public static bool TryParse(string[] args, DateTimeOffset now, out CliOptions? options, out string? error)
    {
        options = null;
        bool execute = args.Contains("--execute");
        var rest = args.Where(a => !string.Equals(a, "--execute", StringComparison.Ordinal)).ToArray();
        var known = execute ? ExecuteParams : DryRunParams;

        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < rest.Length; i += 2)
        {
            string name = rest[i];
            if (!known.Contains(name))
            {
                error = execute && DryRunParams.Contains(name)
                    ? $"{name} нельзя вместе с --execute: отправка идёт только по plan.json"
                    : $"неизвестный параметр {name}";
                return false;
            }
            if (i + 1 >= rest.Length || rest[i + 1].StartsWith("--", StringComparison.Ordinal)) { error = $"нет значения у {name}"; return false; }
            if (!values.TryAdd(name, rest[i + 1])) { error = $"{name} указан дважды"; return false; }
        }

        values.TryGetValue("--target-bootstrap", out var targetBootstrap);
        values.TryGetValue("--target-env", out var targetEnv);
        if (targetBootstrap is not null && targetEnv is not null) { error = "--target-bootstrap и --target-env вместе нельзя, выбери один"; return false; }

        if (execute)
        {
            if (!values.TryGetValue("--plan", out var plan)) { error = "для --execute нужен --plan <папка>/plan.json"; return false; }
            if (targetBootstrap is null && targetEnv is null) { error = "для --execute нужна цель: --target-bootstrap или --target-env"; return false; }
            options = new CliOptions(RunMode.Execute, null, default, default, null, null, null, null,
                targetBootstrap, targetEnv, PartitionStrategy.Crc32, plan, Path.GetDirectoryName(Path.GetFullPath(plan))!);
            error = null;
            return true;
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

        // маршруты и цель идут вместе: без цели нечего проверять в плане, без маршрутов цель не нужна
        values.TryGetValue("--routes", out var routes);
        bool hasTarget = targetBootstrap is not null || targetEnv is not null;
        if (routes is not null && !hasTarget) { error = "с --routes нужна цель: --target-bootstrap или --target-env"; return false; }
        if (routes is null && hasTarget) { error = "цель указана без --routes: план без таблицы маршрутизации не строится"; return false; }

        var partitioner = PartitionStrategy.Crc32;
        if (values.TryGetValue("--partitioner", out var p))
        {
            if (routes is null) { error = "--partitioner имеет смысл только вместе с --routes"; return false; }
            if (!Partitioning.TryParse(p, out partitioner)) { error = $"--partitioner: {p}? есть crc32, murmur2, keep"; return false; }
        }

        values.TryGetValue("--contracts", out var contracts);
        string outDir = values.TryGetValue("--out", out var o) ? o : $"run-{now:yyyyMMdd-HHmmss}";

        options = new CliOptions(RunMode.DryRun, topic, from, to, bootstrap, sourceEnv, contracts, routes,
            targetBootstrap, targetEnv, partitioner, null, outDir);
        error = null;
        return true;
    }

    private static bool TryParseTime(string s, out DateTimeOffset t) =>
        DateTimeOffset.TryParseExact(s, TimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out t);
}
