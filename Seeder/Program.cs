using System.Runtime.CompilerServices;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MessageCheck;

// Наполняет топик тестовыми событиями.
//   dotnet run                                   — учебные события в orders-multi: наши валидные, чужие и битые
//   dotnet run -- <топик>                        — то же в другой топик
//   dotnet run -- --contracts <Dodo.EventRegistry.dll> [<топик>]
//                                                — по одному событию каждого типа из реестра контрактов (настоящие классы),
//                                                  по умолчанию в registry-events

string? contractsPath = null;
var positional = new List<string>();
for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--contracts" && i + 1 < args.Length) contractsPath = args[++i];
    else positional.Add(args[i]);
}
string topic = positional.Count > 0 ? positional[0] : contractsPath is null ? "orders-multi" : "registry-events";
const string bootstrap = "localhost:9092";
const string copyTopic = "orders-copy"; // цель для демо отправки (KafkaReplay/samples/routes.json)

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    foreach (var name in new[] { topic, copyTopic })
    {
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = 3, ReplicationFactor = 1 }]);
            Console.WriteLine($"Создан топик {name} (3 партиции)");
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            Console.WriteLine($"Топик {name} уже есть");
        }
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

(string Key, Headers Headers, string? Payload, string Comment)[] messages = new (string Key, Headers Headers, string? Payload, string Comment)[]
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

if (contractsPath is not null)
{
    if (!ContractsLoader.TryLoad(contractsPath, out var contracts, out var error)) { Console.WriteLine($"ОТКАЗ: {error}"); return 3; }
    messages = FromContracts(contracts!);
}

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
string window = $"--from \"{before.ToString(timeFormat, inv)}\" --to \"{after.ToString(timeFormat, inv)}\"";
if (contractsPath is null)
{
    Console.WriteLine($"  cd ../KafkaReplay/src/KafkaReplay && dotnet run -- --bootstrap {bootstrap} --topic {topic} {window}");
    Console.WriteLine("Проверить и построить план отправки в orders-copy:");
    Console.WriteLine($"  cd ../KafkaReplay/src/KafkaReplay && dotnet run -- --bootstrap {bootstrap} --topic {topic} {window} --routes ../../samples/routes.json --target-bootstrap {bootstrap}");
}
else
    Console.WriteLine($"  cd ../KafkaReplay/src/KafkaReplay && dotnet run -- --bootstrap {bootstrap} --topic {topic} {window} --contracts {Path.GetFullPath(contractsPath)}");
return 0;

// По одному событию каждого типа из реестра: пустой объект класса (все поля по умолчанию), сериализованный
// так же, как его прочитает утилита. проверить, что события настоящих типов читаются и сравниваются
static (string Key, Headers Headers, string? Payload, string Comment)[] FromContracts(LoadedContracts contracts)
{
    var result = new List<(string, Headers, string?, string)>();
    int n = 0;
    foreach (var name in contracts.EventTypesByTopic.Values.SelectMany(x => x).Distinct().Order(StringComparer.Ordinal))
    {
        if (!contracts.Catalog.TryGet(name, out var type)) continue;
        string version = contracts.VersionOf(name) ?? "?";
        try
        {
            object instance = RuntimeHelpers.GetUninitializedObject(type);
            string json = JsonModule.ToJson(instance);
            var headers = new Headers
            {
                { "Event-Type", Encoding.UTF8.GetBytes(name) },
                { "Event-PackageVersion", Encoding.UTF8.GetBytes(version) }
            };
            result.Add(((++n).ToString(), headers, json, $"{name} {version}"));
        }
        catch (Exception e)
        {
            Console.WriteLine($"  пропущен {name}: не удалось создать пример ({e.GetType().Name}: {e.Message})");
        }
    }
    return result.ToArray();
}
