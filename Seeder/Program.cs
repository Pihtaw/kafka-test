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
static Headers Ev(string type, string version) => H(("eventType", T(type)), ("version", T(version)));

var messages = new (string Key, Headers Headers, string? Payload, string Comment)[]
{
    ("101", Ev("OrderCreated", "1"),   """{"orderId":101,"amount":500}""",                    "наше, валидное"),
    ("102", Ev("OrderCreated", "1"),   """{"orderId":102,"amount":300}""",                    "наше, валидное"),
    ("103", Ev("OrderCreated", "2"),   """{"orderId":103,"amount":700,"currency":"RUB"}""",   "наше, валидное v2"),
    ("101", Ev("OrderCancelled", "1"), """{"orderId":101,"reason":"client"}""",              "наше, валидное"),
    ("101", Ev("PaymentCaptured", "1"), """{"paymentId":9001,"orderId":101}""",              "чужое"),
    ("102", H(("eventType", T("CourierAssigned"))), """{"courierId":7,"orderId":102}""",     "чужое, без version"),
    ("104", Ev("OrderCreated", "1"),   """{"orderId":104,"amount":""",                       "битое: JSON оборван"),
    ("105", Ev("OrderCreated", "3"),   """{"orderId":105,"amount":100}""",                    "битое: неизвестная версия"),
    ("106", H(("version", T("1"))),    """{"orderId":106,"amount":100}""",                    "битое: нет eventType"),
    ("107", Ev("", "1"),               """{"orderId":107,"amount":100}""",                    "битое: пустой eventType"),
    ("108", H(("eventType", [0xFF, 0xFE]), ("version", T("1"))), """{"orderId":108,"amount":100}""", "битое: eventType не UTF-8"),
    ("109", H(("eventType", T("OrderCreated")), ("version", T("1")), ("eventType", T("OrderCancelled"))),
                                       """{"orderId":109,"amount":100}""",                    "битое: eventType дважды, разные"),
    ("110", H(("eventType", T("OrderCreated")), ("version", T("1")), ("eventType", T("OrderCreated"))),
                                       """{"orderId":110,"amount":100}""",                    "битое: eventType дважды, одинаковые"),
    ("111", Ev("OrderCreated", "1"),   null,                                                  "tombstone (value = null)"),
    ("112", Ev("OrderCreated", "1"),   "",                                                    "битое: пустое тело"),
};

using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap, Acks = Acks.All }).Build();

var before = DateTimeOffset.Now;
foreach (var m in messages)
{
    var r = await producer.ProduceAsync(topic, new Message<string, string> { Key = m.Key, Value = m.Payload!, Headers = m.Headers });
    Console.WriteLine($"p{r.Partition.Value} o{r.Offset.Value} key={m.Key,-4} {m.Comment}");
}
var after = DateTimeOffset.Now.AddSeconds(1);

Console.WriteLine($"\nОтправлено {messages.Length} сообщений. Прогнать dry-run по ним:");
const string timeFormat = "yyyy-MM-dd HH:mm:sszzz";
var inv = System.Globalization.CultureInfo.InvariantCulture;
Console.WriteLine($"  cd ../DryRun && dotnet run -- {topic} \"{before.ToString(timeFormat, inv)}\" \"{after.ToString(timeFormat, inv)}\"");
