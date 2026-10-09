using System.Text;
using CompatCheck;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Contracts;
using MessageCheck;

// Генератор событий + проверка совместимости версий OrderCreated.
//   dotnet run                    — события генерируются в памяти
//   dotnet run -- kafka           — события публикуются в Kafka и проверяются уже прочитанные оттуда байты
// Отчёт пишется в compat-report.md

const string bootstrap = "localhost:9092";
const string reportPath = "compat-report.md";
bool viaKafka = args.FirstOrDefault() == "kafka";

Console.WriteLine("Версии контракта (сгенерированы ContractGen):");
foreach (var v in OrderCreatedVersions.All)
    Console.WriteLine($"  v{v.Version}: {v.Change}");

var events = EventFactory.ForAll();
Console.WriteLine($"\nСгенерировано событий: {events.Count} " +
                  $"({string.Join(", ", events.GroupBy(e => e.Version).Select(g => $"v{g.Key}: {g.Count()}"))})");

string source = "сгенерированы в памяти";
if (viaKafka)
{
    var fromKafka = await KafkaRoundTrip.RunAsync(bootstrap, events);
    if (fromKafka is null) return 1;
    events = fromKafka.Value.Events;
    source = $"прочитаны из Kafka, топик {fromKafka.Value.Topic}";
}

var runs = new List<(ConsumerMode, List<CheckResult>)>();
foreach (var mode in new[] { Compat.Lenient, Compat.Strict })
{
    var results = await Compat.RunAsync(events, mode);
    runs.Add((mode, results));
    Console.WriteLine($"\nПотребитель: {mode.Name}");
    Console.WriteLine(Report.Matrix(results));
}
Console.WriteLine(Report.Legend);

await File.WriteAllTextAsync(reportPath, Report.Markdown(runs, source));
Console.WriteLine($"\nПодробный отчёт: {Path.GetFullPath(reportPath)}");
return 0;

// публикуем события в Kafka и читаем обратно
static class KafkaRoundTrip
{
    public static async Task<(string Topic, List<GeneratedEvent> Events)?> RunAsync(string bootstrap, List<GeneratedEvent> events)
    {
        string topic = $"order-created-evolution-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}";
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);

        using (var producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig { BootstrapServers = bootstrap, Acks = Acks.All }).Build())
            foreach (var e in events)
                await producer.ProduceAsync(topic, new Message<Null, byte[]>
                {
                    Value = e.Payload,
                    Headers = new Headers
                    {
                        { "eventType", Encoding.UTF8.GetBytes(OrderCreatedVersions.EventType) },
                        { "version", Encoding.UTF8.GetBytes(e.Version.ToString()) },
                        { "case", Encoding.UTF8.GetBytes(e.Case) }
                    }
                });
        Console.WriteLine($"Опубликовано {events.Count} событий в {topic}");

        // читаем обратно через Assign, без коммита
        var tp = new TopicPartition(topic, 0);
        using var consumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"compat-check-{Guid.NewGuid()}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Error
        }).Build();
        consumer.Assign(new TopicPartitionOffset(tp, 0));

        var read = new List<GeneratedEvent>();
        while (read.Count < events.Count && consumer.Consume(TimeSpan.FromSeconds(5)) is { } cr)
        {
            var headers = cr.Message.Headers.Select(h => new MessageCheck.Header(h.Key, h.GetValueBytes())).ToList();
            if (!HeaderReader.TryRead(headers, "version", Reason.MissingVersion, out var version, out var fail) ||
                !HeaderReader.TryRead(headers, "case", Reason.MissingVersion, out var caseName, out fail))
            {
                Console.WriteLine($"ОТКАЗ: offset {cr.Offset.Value}: {fail!.Details}");
                return null;
            }
            read.Add(new GeneratedEvent(int.Parse(version), caseName, cr.Message.Value));
        }
        consumer.Close();

        bool same = read.Count == events.Count && read.Zip(events).All(p => p.First.Payload.AsSpan().SequenceEqual(p.Second.Payload));
        Console.WriteLine($"Прочитано обратно: {read.Count}, байты совпадают с отправленными: {(same ? "да" : "НЕТ")}");
        if (!same) return null;
        return (topic, read);
    }
}
