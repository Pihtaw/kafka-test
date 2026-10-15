using Confluent.Kafka;
using KafkaReplay;
using MessageCheck;

// Прочитать топик за промежуток времени → проверить каждую запись → написать отчёт (без отправки)
// Пример: dotnet run -- --bootstrap localhost:9092 --topic orders-multi --from "2026-10-14 15:00:00+03:00" --to "2026-10-14 15:05:00+03:00"

if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
{
    Console.WriteLine(CliOptions.Usage);
    return args.Length == 0 ? ExitCodes.Blocked : ExitCodes.Ok;
}

if (!CliOptions.TryParse(args, DateTimeOffset.Now, out var opt, out var error))
    return Fail(error!);

// подключение к источнику: локальная Kafka или строка подключения из переменной окружения
ConnectionSettings source;
if (opt!.SourceEnv is not null)
{
    var connectionString = Environment.GetEnvironmentVariable(opt.SourceEnv);
    if (string.IsNullOrWhiteSpace(connectionString)) return Fail($"переменная окружения {opt.SourceEnv} не задана или пустая");
    if (!ConnectionSettings.TryParse(connectionString, out var parsed, out error)) return Fail($"{opt.SourceEnv}: {error}");
    source = parsed!;
}
else source = ConnectionSettings.Plain(opt.Bootstrap!);

var connection = source.ToClientConfig();

long fromMs = opt.From.ToUnixTimeMilliseconds(), toMs = opt.To.ToUnixTimeMilliseconds();

// план чтения
List<PartitionPlan>? plan;
try { plan = await RangePlanner.PlanAsync(connection, opt.Topic, fromMs, toMs); }
catch (KafkaException e) { return Fail($"нет связи с Kafka ({source.Describe()}): {e.Error.Reason}"); }
if (plan is null) return ExitCodes.Blocked;

Console.WriteLine($"Источник: {source.Describe()}");
Console.WriteLine($"Топик {opt.Topic}, время [{opt.From:yyyy-MM-dd HH:mm:ss} ; {opt.To:yyyy-MM-dd HH:mm:ss})");
foreach (var p in plan)
    Console.WriteLine($"  партиция {p.Partition}: low={p.Low} high={p.High} → читаем [{p.Start}; {p.End}) = {p.End - p.Start} шт.");
long expected = plan.Sum(p => p.End - p.Start);

// проверка каждой записи
var processor = new Processor(EventCatalog.Default(), new JsonModule(), TimeSpan.FromMilliseconds(500));
var entries = new List<ReportEntry>();
var finished = new HashSet<int>();

foreach (var cr in RangePlanner.Read(connection, plan, finished))
{
    // из Kafka в MessageCheck в байтах
    var record = new KafkaRecord(
        cr.Message.Headers?.Select(h => new MessageCheck.Header(h.Key, h.GetValueBytes())).ToList() ?? [],
        cr.Message.Key,
        cr.Message.Value);

    var decision = await processor.ProcessAsync(record);
    long ts = cr.Message.Timestamp.UnixTimestampMs;
    entries.Add(ReportEntry.From(cr, record, decision, outsideWindow: ts < fromMs || ts >= toMs));
}

// сводка
var byVerdict = entries.GroupBy(e => e.Verdict).ToDictionary(g => g.Key, g => g.Count());
var byReason = entries.GroupBy(e => e.Reason).ToDictionary(g => g.Key, g => g.Count());
var byEventType = entries.GroupBy(e => e.EventType ?? "—")
    .ToDictionary(g => g.Key, g => g.GroupBy(e => e.Verdict).ToDictionary(v => v.Key, v => v.Count()));

bool allRead = finished.Count == plan.Count;               // каждая партиция дошла до End
long gaps = expected - entries.Count;
bool sumMatches = byVerdict.Values.Sum() == entries.Count;  // у каждой записи ровно одно решение
int rejected = entries.Count(e => e.IsRejected);
int exitCode = ExitCodes.For(allRead, sumMatches, rejected);

Console.WriteLine();
foreach (var e in entries)
    Console.WriteLine($"  p{e.Partition} o{e.Offset,-3} {e.EventType ?? "—",-42} {e.Verdict,-15} {e.Reason}{(e.OutsideWindow ? "  ⚠ время вне окна" : "")}");

Console.WriteLine($"\nПрочитано {entries.Count} записей, offsets в диапазоне: {expected}" + (gaps > 0 ? $" (без записей: {gaps})" : ""));
Console.WriteLine($"Дочитаны партиции: {finished.Count} из {plan.Count}");
foreach (var (verdict, count) in byVerdict.OrderBy(x => x.Key)) Console.WriteLine($"  {verdict,-15} {count}");
Console.WriteLine(allRead && sumMatches ? "✅ Сводка сходится" : "❌ Сводка НЕ сходится");

// отчет
var report = new
{
    mode = "dry-run (ничего не отправлялось)",
    source = source.Describe(),
    topic = opt.Topic,
    window = new { from = opt.From, to = opt.To, fromMs, toMs },
    partitions = plan,
    summary = new { read = entries.Count, offsetsInRange = expected, gaps, partitionsFinished = finished.Count, allRead, sumMatches, rejected, byVerdict, byReason, byEventType },
    exitCode,
    records = entries
};
string reportPath = await ReportWriter.WriteAsync(opt.OutDir, report);
Console.WriteLine($"Отчёт: {reportPath}");
Console.WriteLine($"Код выхода {exitCode}: {ExitCodes.Describe(exitCode)}");
return exitCode;

static int Fail(string message)
{
    Console.WriteLine($"ОТКАЗ: {message}");
    Console.WriteLine();
    Console.WriteLine(CliOptions.Usage);
    return ExitCodes.Blocked;
}
