using System.CommandLine;
using System.CommandLine.Parsing;
using System.Globalization;

namespace KafkaReplay;

// Командная строка на System.CommandLine, две команды:
//   replay check   — прочитать окно, проверить, написать отчёт и (с --routes) план отправки; ничего не отправляет
//   replay execute — отправить по сохранённому plan.json и побайтово сверить; источник заново не читается
// System.CommandLine разбирает сами значения: обязательные параметры, формат времени, --tail, --partitioner,
// неизвестные параметры, повторы — и строит --help. Связи между параметрами («одно из двух», «вместе нельзя»,
// «с — раньше по») проверяем после разбора сами: так сообщения остаются нашими и понятными
public sealed class ReplayCli
{
    // check: источник и окно
    private readonly Option<string> _topic = new("--topic") { Description = "топик-источник", Required = true, HelpName = "топик" };
    private readonly Option<DateTimeOffset> _from = TimeOption("--from", "начало окна, включительно");
    private readonly Option<DateTimeOffset> _to = TimeOption("--to", "конец окна, не включая");
    private readonly Option<string> _bootstrap = new("--bootstrap")
        { Description = "источник — Kafka без входа, например localhost:9092", HelpName = "host:port" };
    private readonly Option<string> _sourceEnv = new("--source-env")
        { Description = "источник — переменная окружения со строкой подключения Dodo.Kafka: BootstrapServers=...;Type=EventHubSas;...", HelpName = "ПЕРЕМЕННАЯ" };
    private readonly Option<string> _contracts = new("--contracts")
        { Description = "сборка реестра контрактов (Dodo.EventRegistry.dll); без неё — встроенные учебные контракты", HelpName = "dll" };
    private readonly Option<long?> _tail = new("--tail")
    {
        Description = $"сколько записей после конца окна просмотреть в каждой партиции в поисках «опоздавших»: число, 0 — не смотреть, all — до конца (по умолчанию {CliOptions.DefaultTail})",
        HelpName = "N|all",
        CustomParser = ParseTail
    };
    private readonly Option<string> _out;

    // check: план отправки
    private readonly Option<string> _routes = new("--routes")
        { Description = "таблица маршрутизации «тип события → топик»; с ней рядом с отчётом пишется plan.json", HelpName = "routes.json" };
    private readonly Option<string> _checkTargetBootstrap = TargetBootstrap();
    private readonly Option<string> _checkTargetEnv = TargetEnv();
    private readonly Option<PartitionStrategy?> _partitioner = new("--partitioner")
    {
        Description = "как выбрать партицию в цели: crc32 (как .NET, по умолчанию), murmur2 (как Java), keep (как в источнике)",
        HelpName = "crc32|murmur2|keep",
        CustomParser = ParsePartitioner
    };

    // execute
    private readonly Option<string> _plan = new("--plan") { Description = "путь к plan.json, построенному командой check", Required = true, HelpName = "plan.json" };
    private readonly Option<string> _executeTargetBootstrap = TargetBootstrap();
    private readonly Option<string> _executeTargetEnv = TargetEnv();

    public ReplayCli(DateTimeOffset now)
    {
        _out = new Option<string>("--out")
        {
            Description = "папка для отчёта и плана; по умолчанию — новая, с датой и временем запуска",
            HelpName = "папка",
            DefaultValueFactory = _ => $"run-{now:yyyyMMdd-HHmmss}"
        };

        Check = new Command("check", "Прочитать окно топика, проверить события контрактами, написать отчёт и (с --routes) план отправки. Ничего не отправляет.")
        {
            _topic, _from, _to, _bootstrap, _sourceEnv, _contracts, _tail, _out,
            _routes, _checkTargetBootstrap, _checkTargetEnv, _partitioner
        };
        Execute = new Command("execute", "Отправить записи строго по plan.json и побайтово сверить отправленное. Источник заново не читается.")
        {
            _plan, _executeTargetBootstrap, _executeTargetEnv
        };

        Root = new RootCommand("replay — переналивка событий Kafka с проверкой контрактами");
        Root.Subcommands.Add(Check);
        Root.Subcommands.Add(Execute);

        Check.SetAction((parseResult, _) =>
            TryCheckOptions(parseResult, out var options, out var error) ? DryRunCommand.RunAsync(options!) : Task.FromResult(Cli.Fail(error!)));
        Execute.SetAction((parseResult, _) =>
            TryExecuteOptions(parseResult, out var options, out var error) ? ExecuteCommand.RunAsync(options!) : Task.FromResult(Cli.Fail(error!)));
    }

    public RootCommand Root { get; }
    public Command Check { get; }
    public Command Execute { get; }

    // Разбор без запуска — для тестов: ошибки System.CommandLine и наши проверки связей одним текстом
    public bool TryParse(string[] args, out CliOptions? options, out string? error)
    {
        options = null;
        var result = Root.Parse(args);
        if (result.Errors.Count > 0)
        {
            error = Describe(result.Errors);
            return false;
        }
        if (ReferenceEquals(result.CommandResult.Command, Check)) return TryCheckOptions(result, out options, out error);
        if (ReferenceEquals(result.CommandResult.Command, Execute)) return TryExecuteOptions(result, out options, out error);

        error = "укажи команду: check или execute";
        return false;
    }

    // каждая ошибка с новой строки
    public static string Describe(IEnumerable<ParseError> errors) => string.Join(Environment.NewLine + "       ", errors.Select(e => e.Message));

    private bool TryCheckOptions(ParseResult r, out CliOptions? options, out string? error)
    {
        options = null;
        error = CheckProblem(r);
        if (error is not null) return false;

        options = new CliOptions(RunMode.DryRun, r.GetValue(_topic), r.GetValue(_from), r.GetValue(_to),
            r.GetValue(_bootstrap), r.GetValue(_sourceEnv), r.GetValue(_contracts), r.GetValue(_routes),
            r.GetValue(_checkTargetBootstrap), r.GetValue(_checkTargetEnv), r.GetValue(_partitioner) ?? PartitionStrategy.Crc32,
            null, r.GetValue(_out)!, r.GetValue(_tail) ?? CliOptions.DefaultTail);
        return true;
    }

    // связи между параметрами check; null — всё в порядке
    private string? CheckProblem(ParseResult r)
    {
        string? routes = r.GetValue(_routes);
        bool hasSource = r.GetValue(_bootstrap) is not null;
        bool hasSourceEnv = r.GetValue(_sourceEnv) is not null;
        bool hasTargetBootstrap = r.GetValue(_checkTargetBootstrap) is not null;
        bool hasTargetEnv = r.GetValue(_checkTargetEnv) is not null;

        if (string.IsNullOrWhiteSpace(r.GetValue(_topic))) return "--topic пустой";
        if (r.GetValue(_from) >= r.GetValue(_to)) return "--from должно быть раньше --to";
        if (!hasSource && !hasSourceEnv) return "укажи источник: --bootstrap или --source-env";
        if (hasSource && hasSourceEnv) return "--bootstrap и --source-env вместе нельзя, выбери один";
        if (hasTargetBootstrap && hasTargetEnv) return "--target-bootstrap и --target-env вместе нельзя, выбери один";
        // маршруты и цель идут вместе: без цели нечего проверять в плане, без маршрутов цель не нужна
        if (routes is not null && !hasTargetBootstrap && !hasTargetEnv) return "с --routes нужна цель: --target-bootstrap или --target-env";
        if (routes is null && (hasTargetBootstrap || hasTargetEnv)) return "цель указана без --routes: план без таблицы маршрутизации не строится";
        if (routes is null && r.GetValue(_partitioner) is not null) return "--partitioner имеет смысл только вместе с --routes";
        return null;
    }

    private bool TryExecuteOptions(ParseResult r, out CliOptions? options, out string? error)
    {
        options = null;
        string plan = r.GetValue(_plan)!;
        string? targetBootstrap = r.GetValue(_executeTargetBootstrap);
        string? targetEnv = r.GetValue(_executeTargetEnv);

        if (targetBootstrap is null && targetEnv is null) { error = "укажи цель: --target-bootstrap или --target-env"; return false; }
        if (targetBootstrap is not null && targetEnv is not null) { error = "--target-bootstrap и --target-env вместе нельзя, выбери один"; return false; }

        options = new CliOptions(RunMode.Execute, null, default, default, null, null, null, null,
            targetBootstrap, targetEnv, PartitionStrategy.Crc32, plan, Path.GetDirectoryName(Path.GetFullPath(plan))!);
        error = null;
        return true;
    }

    private static Option<DateTimeOffset> TimeOption(string name, string description) => new(name)
    {
        Description = $"{description}; формат {CliOptions.TimeFormat}, например \"2026-10-14 15:00:00+03:00\"",
        HelpName = "время",
        Required = true,
        CustomParser = r => ParseTime(r, name)
    };

    private static Option<string> TargetBootstrap() => new("--target-bootstrap")
        { Description = "цель — Kafka без входа", HelpName = "host:port" };

    private static Option<string> TargetEnv() => new("--target-env")
        { Description = "цель — переменная окружения со строкой подключения Dodo.Kafka", HelpName = "ПЕРЕМЕННАЯ" };

    private static DateTimeOffset ParseTime(ArgumentResult r, string name)
    {
        string text = r.Tokens.Single().Value;
        if (DateTimeOffset.TryParseExact(text, CliOptions.TimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var t)) return t;
        r.AddError($"{name}: «{text}» — время должно быть в формате {CliOptions.TimeFormat}, например 2026-10-14 15:00:00+03:00");
        return default;
    }

    private static long? ParseTail(ArgumentResult r)
    {
        string text = r.Tokens.Single().Value;
        if (CliOptions.TryParseTail(text, out long tail)) return tail;
        r.AddError($"--tail: «{text}» — нужно число ≥ 0 или all");
        return null;
    }

    private static PartitionStrategy? ParsePartitioner(ArgumentResult r)
    {
        string text = r.Tokens.Single().Value;
        if (Partitioning.TryParse(text, out var strategy)) return strategy;
        r.AddError($"--partitioner: «{text}» — есть crc32, murmur2, keep");
        return null;
    }
}
