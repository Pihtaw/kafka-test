using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

// Идея мультитопика, (1) наши валидные, (2) чужие, (3) юитые

string topic = args.Length > 0 ? args[0] : "orders-multi";
const string bootstrap = "localhost:9092";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }]);
        Console.WriteLine($"Создан топик {topic} (3 партиции)");
    }
    catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
    {
        Console.WriteLine($"Топик {topic} уже есть");
    }
}

static Headers H(params (string Key, byte[]? Value)[] items)
{
    var h = new Headers();
    foreach (var (k, v) in items) h.Add(k, v);
    return h;
}
static byte[] T(string s) => Encoding.UTF8.GetBytes(s);
const string EventType = "Event-Type";
const string PackageVersion = "Event-PackageVersion";
static Headers Ev(string type, string version) => H((EventType, T(type)), (PackageVersion, T(version)));

const string OrderCreated = "MessageCheck.SampleContracts.OrderCreated";
const string OrderCancelled = "MessageCheck.SampleContracts.OrderCancelled";

var messages = new (string Key, Headers Headers, string? Payload, string Comment)[]
{
    ("101", Ev(OrderCreated, "1.0.0"),   """{"OrderId":101,"Amount":500}""",                    "наше, пакет 1.0.0"),
    ("102", Ev(OrderCreated, "1.0.0"),   """{"OrderId":102,"Amount":300,"Comment":"позвонить"}""", "наше, пакет 1.0.0, лишнее поле Comment пропускается"),
    ("103", Ev(OrderCreated, "1.1.0"),   """{"OrderId":103,"Amount":700,"Currency":"RUB"}""",   "наше, пакет 1.1.0 (добавилось Currency)"),
    ("101", Ev(OrderCancelled, "1.0.0"), """{"OrderId":101,"Reason":"client"}""",              "наше"),
    ("101", Ev("Dodo.Payments.Contracts.PaymentCaptured", "3.2.0"), """{"PaymentId":9001,"OrderId":101}""", "чужое"),
    ("102", H((EventType, T("Dodo.Delivery.Contracts.CourierAssigned"))), """{"CourierId":7,"OrderId":102}""", "чужое, без версии"),
    ("104", Ev(OrderCreated, "1.0.0"),   """{"OrderId":104,"Amount":""",                       "битое: JSON оборван"),
    ("105", Ev(OrderCreated, "2.0.0"),   """{"OrderId":105,"Amount":100}""",                    "наше, незнакомая версия пакета — тип тот же, читается"),
    ("106", H((PackageVersion, T("1.0.0"))), """{"OrderId":106,"Amount":100}""",                "битое: нет Event-Type"),
    ("107", Ev("", "1.0.0"),             """{"OrderId":107,"Amount":100}""",                    "битое: пустой Event-Type"),
    ("108", H((EventType, [0xFF, 0xFE]), (PackageVersion, T("1.0.0"))), """{"OrderId":108,"Amount":100}""", "битое: Event-Type не UTF-8"),
    ("109", H((EventType, T(OrderCreated)), (PackageVersion, T("1.0.0")), (EventType, T(OrderCancelled))),
                                         """{"OrderId":109,"Amount":100}""",                    "битое: Event-Type дважды, разные"),
    ("110", H((EventType, T(OrderCreated)), (PackageVersion, T("1.0.0")), (EventType, T(OrderCreated))),
                                         """{"OrderId":110,"Amount":100}""",                    "битое: Event-Type дважды, одинаковые"),
    ("111", Ev(OrderCreated, "1.0.0"),   null,                                                  "tombstone (value = null)"),
    ("111", Ev(OrderCreated, "1.0.0"),   "",                                                    "битое: пустое тело (тот же ключ, что у tombstone)"),
};

using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap, Acks = Acks.All }).Build();

var before = DateTimeOffset.Now;
foreach (var m in messages)
{
    var r = await producer.ProduceAsync(topic, new Message<string, string> { Key = m.Key, Value = m.Payload!, Headers = m.Headers });
    Console.WriteLine($"p{r.Partition.Value} o{r.Offset.Value} key={m.Key,-4} {m.Comment}");
}
var after = DateTimeOffset.Now.AddSeconds(1);
Console.WriteLine($"\nОтправлено {messages.Length} сообщений. Проверить их утилитой:");
const string timeFormat = "yyyy-MM-dd HH:mm:sszzz";
var inv = System.Globalization.CultureInfo.InvariantCulture;
Console.WriteLine($"  cd ../KafkaReplay/src/KafkaReplay && dotnet run -- --bootstrap {bootstrap} --topic {topic} --from \"{before.ToString(timeFormat, inv)}\" --to \"{after.ToString(timeFormat, inv)}\"");
