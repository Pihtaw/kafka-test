using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MessageCheck;

// прочитать за промежуток времени -> проверить каждую запись -> написать отчет (нет отправки)
// время печатаю в Seeder, взять нужное dotnet run -- orders-multi "2026-10-05 18:00:00" "2026-10-05 18:05:00"

const string TimeFormat = "yyyy-MM-dd HH:mm:sszzz";

if (args.Length < 3)
{
    Console.WriteLine($"dotnet run -- <топик> \"<с>\" \"<по>\" [report.json]   время в формате {TimeFormat}, например 2026-10-05 18:00:00+03:00");
    return 1;
}

string topic = args[0];
if (!TryParseTime(args[1], out var fromTime) || !TryParseTime(args[2], out var toTime))
{
    Console.WriteLine($"ОТКАЗ: время должно быть в формате {TimeFormat}, например 2026-10-05 18:00:00+03:00");
    return 1;
}
string reportPath = args.Length > 3 ? args[3] : "report.json";
long fromMs = fromTime.ToUnixTimeMilliseconds(), toMs = toTime.ToUnixTimeMilliseconds();
const string bootstrap = "localhost:9092";

if (fromMs >= toMs) { Console.WriteLine("ОТКАЗ: «с» должно быть раньше «по»"); return 1; }

// чтение
var plan = await RangePlanner.PlanAsync(bootstrap, topic, fromMs, toMs);
if (plan is null) return 1;

Console.WriteLine($"Топик {topic}, время [{fromTime:yyyy-MM-dd HH:mm:ss} ; {toTime:yyyy-MM-dd HH:mm:ss})");
foreach (var p in plan)
    Console.WriteLine($"  партиция {p.Partition}: low={p.Low} high={p.High} → читаем [{p.Start}; {p.End}) = {p.End - p.Start} шт.");
long expected = plan.Sum(p => p.End - p.Start);

// проверка
var processor = new Processor(EventCatalog.Default(), new JsonModule(), TimeSpan.FromMilliseconds(500));
var entries = new List<ReportEntry>();

var finished = new HashSet<int>();
foreach (var cr in RangePlanner.Read(bootstrap, plan, finished))
{
    // из кафка в MessageCheck в байтах
    var record = new KafkaRecord(
        cr.Message.Headers?.Select(h => new MessageCheck.Header(h.Key, h.GetValueBytes())).ToList() ?? [],
        cr.Message.Key,
        cr.Message.Value);

    var decision = await processor.ProcessAsync(record);
    long ts = cr.Message.Timestamp.UnixTimestampMs;
    entries.Add(ReportEntry.From(cr, record, decision, outsideWindow: ts < fromMs || ts >= toMs));
}

var byVerdict = entries.GroupBy(e => e.Verdict).ToDictionary(g => g.Key, g => g.Count());
var byReason = entries.GroupBy(e => e.Reason).ToDictionary(g => g.Key, g => g.Count());
var byEventType = entries.GroupBy(e => e.EventType ?? "—")
    .ToDictionary(g => g.Key, g => g.GroupBy(e => e.Verdict).ToDictionary(v => v.Key, v => v.Count()));

bool allRead = finished.Count == plan.Count;   // каждая партиция дошла до End
long gaps = expected - entries.Count;
bool sumMatches = byVerdict.Values.Sum() == entries.Count;  // у каждой записи ровно одно решение

Console.WriteLine();
foreach (var e in entries)
    Console.WriteLine($"  p{e.Partition} o{e.Offset,-3} {e.EventType ?? "—",-16} {e.Verdict,-15} {e.Reason}{(e.OutsideWindow ? "  ⚠ время вне окна" : "")}");

Console.WriteLine($"\nПрочитано {entries.Count} записей, offsets в диапазоне: {expected}" + (gaps > 0 ? $" (без записей: {gaps})" : ""));
Console.WriteLine($"Дочитаны партиции: {finished.Count} из {plan.Count}");
foreach (var (verdict, count) in byVerdict.OrderBy(x => x.Key)) Console.WriteLine($"  {verdict,-15} {count}");
Console.WriteLine(allRead && sumMatches ? "✅ Сводка сходится" : "❌ Сводка НЕ сходится");

// отчет
var report = new
{
    mode = "dry-run (ничего не отправлялось)",
    topic,
    window = new { from = fromTime, to = toTime, fromMs, toMs },
    partitions = plan,
    summary = new { read = entries.Count, offsetsInRange = expected, gaps, partitionsFinished = finished.Count, allRead, sumMatches, byVerdict, byReason, byEventType },
    records = entries
};
var json = JsonSerializer.Serialize(report, new JsonSerializerOptions
{
    WriteIndented = true,
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All) // кириллица в отчёте читается как есть
});
await File.WriteAllTextAsync(reportPath, json);
Console.WriteLine($"Отчёт: {Path.GetFullPath(reportPath)}");
return allRead && sumMatches ? 0 : 2;

static bool TryParseTime(string s, out DateTimeOffset t) =>
DateTimeOffset.TryParseExact(s, TimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out t);

// План чтения одной партиции: границы фиксируются до чтения.
record PartitionPlan(string Topic, int Partition, long Low, long High, long Start, long End);

// оформление строки отчета
record ReportEntry(
    int Partition, long Offset, DateTimeOffset Timestamp, string TimestampType, bool OutsideWindow,
    string? Key, List<ReportHeader> Headers, string? Payload, string Sha256,
    string? EventType, string Verdict, string Reason, string? Details, string? ResultJson)
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static ReportEntry From(ConsumeResult<byte[], byte[]> cr, KafkaRecord r, Decision d, bool outsideWindow) => new(
        cr.Partition.Value, cr.Offset.Value,
        DateTimeOffset.FromUnixTimeMilliseconds(cr.Message.Timestamp.UnixTimestampMs).ToLocalTime(),
        cr.Message.Timestamp.Type.ToString(), outsideWindow,
        r.Key is null ? null : Text(r.Key),
        r.Headers.Select(h => new ReportHeader(h.Key, h.Value is null ? null : Text(h.Value), h.Value is null ? null : Convert.ToHexString(h.Value))).ToList(),
        r.Payload is null ? null : Text(r.Payload),
        Checksum(r),
        r.Headers.Where(h => h.Key == "eventType" && h.Value is { Length: > 0 }).Select(h => Text(h.Value!)).FirstOrDefault(),
        d.Verdict.ToString(), d.Reason.ToString(), d.Details, d.ResultJson);

    // текст если это UTF-8
    private static string Text(byte[] bytes)
    {
        try { return StrictUtf8.GetString(bytes); }
        catch (DecoderFallbackException) { return $"<не UTF-8: {Convert.ToHexString(bytes)}>"; }
    }

    // контрольная сумма записи
    private static string Checksum(KafkaRecord r)
    {
        using var ms = new MemoryStream();
        void Put(byte[]? b)
        {
            ms.Write(BitConverter.GetBytes(b?.Length ?? -1));
            if (b is not null) ms.Write(b);
        }
        Put(r.Key);
        ms.Write(BitConverter.GetBytes(r.Headers.Count));
        foreach (var h in r.Headers) { Put(Encoding.UTF8.GetBytes(h.Key)); Put(h.Value); }
        Put(r.Payload);
        return Convert.ToHexString(SHA256.HashData(ms.ToArray())).ToLowerInvariant();
    }
}

record ReportHeader(string Key, string? Value, string? Hex);

// чтение диапазона как в RangeReader только байты
static class RangePlanner
{
    public static async Task<List<PartitionPlan>?> PlanAsync(string bootstrap, string topic, long fromMs, long toMs)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        var meta = admin.GetMetadata(topic, TimeSpan.FromSeconds(5)).Topics[0];
        if (meta.Error.IsError) { Console.WriteLine($"ОТКАЗ: топик '{topic}' недоступен: {meta.Error.Reason}"); return null; }

        var partitions = meta.Partitions.Select(p => new TopicPartition(topic, p.PartitionId)).OrderBy(tp => tp.Partition.Value).ToList();

        async Task<Dictionary<int, long>> List(OffsetSpec spec)
        {
            var res = await admin.ListOffsetsAsync(partitions.Select(tp => new TopicPartitionOffsetSpec { TopicPartition = tp, OffsetSpec = spec }));
            return res.ResultInfos.ToDictionary(i => i.TopicPartitionOffsetError.Partition.Value, i => i.TopicPartitionOffsetError.Offset.Value);
        }

        var low = await List(OffsetSpec.Earliest());
        var high = await List(OffsetSpec.Latest());
        var from = await List(OffsetSpec.ForTimestamp(fromMs));
        var to = await List(OffsetSpec.ForTimestamp(toMs));

        return partitions.Select(tp =>
        {
            int p = tp.Partition.Value;
            long start = from[p] == Offset.End.Value ? high[p] : from[p];   // End → high: после T ничего нет
            long end = to[p] == Offset.End.Value ? high[p] : to[p];
            start = Math.Clamp(start, low[p], high[p]);
            end = Math.Clamp(end, start, high[p]);
            return new PartitionPlan(topic, p, low[p], high[p], start, end);
        }).ToList();
    }

    // без коммита, читаем [Start; End) через Assign
    public static IEnumerable<ConsumeResult<byte[], byte[]>> Read(string bootstrap, List<PartitionPlan> plan, ISet<int> finished)
    {
        foreach (var p in plan.Where(p => p.End <= p.Start)) finished.Add(p.Partition);
        var todo = plan.Where(p => p.End > p.Start).ToList();
        if (todo.Count == 0) yield break;

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"kafka-tools-dry-run-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}",
            AutoOffsetReset = AutoOffsetReset.Error,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true
        };
        using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        consumer.Assign(todo.Select(p => new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition), p.Start)));

        var endOf = todo.ToDictionary(p => p.Partition, p => p.End);
        var notDone = todo.Select(p => p.Partition).ToHashSet();

        while (notDone.Count > 0)
        {
            ConsumeResult<byte[], byte[]>? cr;
            try { cr = consumer.Consume(TimeSpan.FromSeconds(5)); }
            catch (ConsumeException e)
            {
                Console.WriteLine($"ОШИБКА чтения: {e.Error.Code} {e.Error.Reason}. Не дочитаны партиции {string.Join(", ", notDone)}");
                break;
            }
            if (cr is null) { Console.WriteLine($"Таймаут: не дочитаны партиции {string.Join(", ", notDone)}"); break; }

            int p = cr.Partition.Value;
            if (cr.IsPartitionEOF) { if (cr.Offset.Value >= endOf[p]) Done(p, cr.TopicPartition); continue; }
            if (cr.Offset.Value >= endOf[p]) { Done(p, cr.TopicPartition); continue; }
            yield return cr;
            if (cr.Offset.Value >= endOf[p] - 1) Done(p, cr.TopicPartition);
        }
        consumer.Close();

        void Done(int p, TopicPartition tp)
        {
            if (!notDone.Remove(p)) return;
            finished.Add(p);
            consumer.Pause([tp]);
        }
    }
}
