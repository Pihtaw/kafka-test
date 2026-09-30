using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

if (args.Length < 3)
{
    Console.WriteLine("Использование: dotnet run -- <топик> \"<с>\" \"<по>\"");
    Console.WriteLine("Пример:        dotnet run -- test-topic \"2026-09-30 18:00:00\" \"2026-09-30 18:05:00\"");
    return;
}

string topic = args[0];
DateTimeOffset fromTime = DateTimeOffset.Parse(args[1]);
DateTimeOffset toTime = DateTimeOffset.Parse(args[2]);
long fromMs = fromTime.ToUnixTimeMilliseconds();
long toMs = toTime.ToUnixTimeMilliseconds();
const string groupId = "test-group";
const string bootstrap = "localhost:9092";

Console.WriteLine($"Задача: {topic}, время [{fromTime:yyyy-MM-dd HH:mm:ss} ; {toTime:yyyy-MM-dd HH:mm:ss})");
Console.WriteLine($"        в миллисекундах [{fromMs} ; {toMs})\n");

if (fromMs >= toMs)
{
    Console.WriteLine("ОТКАЗ: время «с» должно быть раньше времени «по»");
    return;
}

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

var topicMeta = admin.GetMetadata(topic, TimeSpan.FromSeconds(5)).Topics[0];
if (topicMeta.Error.IsError)
{
    Console.WriteLine($"ОТКАЗ: топик '{topic}' недоступен: {topicMeta.Error.Reason}");
    return;
}

// список партиций топика
var partitions = topicMeta.Partitions
    .Select(p => new TopicPartition(topic, p.PartitionId))
    .OrderBy(tp => tp.Partition.Value)
    .ToList();

Dictionary<int, long> low, high, fromOffsets, toOffsets;
try
{
    low = await ListOffsets(OffsetSpec.Earliest());
    high = await ListOffsets(OffsetSpec.Latest());
    fromOffsets = await ListOffsets(OffsetSpec.ForTimestamp(fromMs));
    toOffsets = await ListOffsets(OffsetSpec.ForTimestamp(toMs));
}
catch (ListOffsetsException ex)
{
    Console.WriteLine($"ОТКАЗ: брокер не смог посчитать offsets: {ex.Message}");
    return;
}

var plan = new List<(TopicPartition Tp, long Start, long End)>();

Console.WriteLine("План по партициям (границы зафиксированы до чтения):");
Console.WriteLine("партиция | low | high | с (offset) | по (offset, не вкл.) | сообщений");
foreach (var tp in partitions)
{
    int p = tp.Partition.Value;

    long start = fromOffsets[p] == Offset.End.Value ? high[p] : fromOffsets[p];
    long end = toOffsets[p] == Offset.End.Value ? high[p] : toOffsets[p];

    start = Math.Clamp(start, low[p], high[p]);
    end = Math.Clamp(end, start, high[p]);

    long count = end - start;
    Console.WriteLine($"  {p,8} | {low[p],3} | {high[p],4} | {start,10} | {end,20} | {count,9}");

    if (count > 0) plan.Add((tp, start, end));
}

if (plan.Count == 0)
{
    Console.WriteLine("\nВ этом промежутке времени сообщений нет — читать нечего.");
    return;
}

var before = GetCommitted(groupId, partitions);
Console.WriteLine($"\n[ДО]    закладки группы '{groupId}': {Format(before)}");

var config = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = groupId,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false
};
using var consumer = new ConsumerBuilder<string, string>(config).Build();

// каждая партиция со своего стартового offset
consumer.Assign(plan.Select(x => new TopicPartitionOffset(x.Tp, x.Start)));

var endOf = plan.ToDictionary(x => x.Tp.Partition.Value, x => x.End);
var readCount = plan.ToDictionary(x => x.Tp.Partition.Value, _ => 0L);
var notDone = plan.Select(x => x.Tp.Partition.Value).ToHashSet();
int outOfWindow = 0;

Console.WriteLine();
while (notDone.Count > 0)
{
    var r = consumer.Consume(TimeSpan.FromSeconds(5));
    if (r == null)
    {
        Console.WriteLine($"Таймаут: не дочитаны партиции {string.Join(", ", notDone)}");
        break;
    }

    int p = r.Partition.Value;
    long o = r.Offset.Value;

    // дошли до конца партиции
    if (o >= endOf[p])
    {
        MarkDone(p, r.TopicPartition);
        continue;
    }

    readCount[p]++;

    long ts = r.Message.Timestamp.UnixTimestampMs;
    bool inWindow = ts >= fromMs && ts < toMs;
    if (!inWindow) outOfWindow++;
    string time = DateTimeOffset.FromUnixTimeMilliseconds(ts).ToLocalTime().ToString("HH:mm:ss.fff");

    var headers = r.Message.Headers == null
        ? ""
        : string.Join(", ", r.Message.Headers.Select(h =>
            $"{h.Key}={(h.GetValueBytes() is { } b ? Encoding.UTF8.GetString(b) : "null")}"));

    Console.WriteLine($"  p{p} o{o} | {time} ({r.Message.Timestamp.Type}){(inWindow ? "" : "вне окна")} " +
                      $"| key={r.Message.Key} | {headers} | {r.Message.Value}");

    if (o == endOf[p] - 1) MarkDone(p, r.TopicPartition);
}

consumer.Close();

Console.WriteLine("\nСводка по партициям:");
foreach (var x in plan)
{
    int p = x.Tp.Partition.Value;
    long expected = x.End - x.Start;
    Console.WriteLine($"партиция {p}: прочитано {readCount[p]} из {expected}{(readCount[p] == expected ? "" : "  - НЕ СОВПАДАЕТ")}");
}
Console.WriteLine($"всего: {readCount.Values.Sum()} из {plan.Sum(x => x.End - x.Start)}");
if (outOfWindow > 0)
    Console.WriteLine($"сообщений со временем вне окна: {outOfWindow} (CreateTime от продюсера идёт не строго по порядку)");

var after = GetCommitted(groupId, partitions);
Console.WriteLine($"\n[ПОСЛЕ] закладки группы '{groupId}': {Format(after)}");
Console.WriteLine(Format(before) == Format(after)
    ? "Закладки группы не изменились"
    : "Закладки группы изменились!");

async Task<Dictionary<int, long>> ListOffsets(OffsetSpec spec)
{
    var request = partitions.Select(tp => new TopicPartitionOffsetSpec { TopicPartition = tp, OffsetSpec = spec });
    var result = await admin.ListOffsetsAsync(request);
    return result.ResultInfos.ToDictionary(
        info => info.TopicPartitionOffsetError.Partition.Value,
        info => info.TopicPartitionOffsetError.Offset.Value);
}

void MarkDone(int partition, TopicPartition tp)
{
    if (notDone.Remove(partition)) consumer.Pause(new[] { tp });
}

// Закладки группы по всем партициям — отдельным временным консьюмером.
static Dictionary<int, string> GetCommitted(string groupId, List<TopicPartition> partitions)
{
    var cfg = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = groupId, EnableAutoCommit = false };
    using var c = new ConsumerBuilder<Ignore, Ignore>(cfg).Build();
    return c.Committed(partitions, TimeSpan.FromSeconds(5)).ToDictionary(
        x => x.Partition.Value,
        x => x.Offset == Offset.Unset ? "нет" : x.Offset.Value.ToString());
}

static string Format(Dictionary<int, string> offsets)
    => string.Join(", ", offsets.OrderBy(x => x.Key).Select(x => $"p{x.Key}={x.Value}"));
