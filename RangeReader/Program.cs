using System.Text;
using Confluent.Kafka;

// dotnet run -- <топик> <партиция> <from> <to>
string topic  = args.Length > 0 ? args[0] : "test-topic";
int partition = args.Length > 1 ? int.Parse(args[1]) : 0;
long from     = args.Length > 2 ? long.Parse(args[2]) : 2;
long to       = args.Length > 3 ? long.Parse(args[3]) : 5;
const string groupId = "test-group";

Console.WriteLine($"Задача: прочитать {topic}, партиция {partition}, offsets {from}..{to} включительно\n");

var tp = new TopicPartition(topic, partition); // адрес одной партиции

// группа ДО 
var before = GetCommitted(groupId, tp);
Console.WriteLine($"ДО offset группы '{groupId}': {before}");

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = groupId,             // без него библиотека не создает консьюмер, а мы по нему ищем закладку
    // но при Assign консьюмер не вступает в группу и не вызывает ребалансировку
    EnableAutoCommit = false,      // автоматический коммит выключаем
    EnableAutoOffsetStore = false
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
long low = wm.Low.Value;
long high = wm.High.Value;
Console.WriteLine($"low={low}, high={high} - существуют offsets {low}..{high - 1}");

if (from > to)
{
    Console.WriteLine($"начало диапазона ({from}) больше конца ({to})");
    return;
}
if (low == high)
{
    Console.WriteLine("партиция пустая");
    return;
}
if (from < low)
{
    Console.WriteLine($"offset {from} уже удалён (первый доступный — {low})");
    return;
}
if (to >= high)
{
    Console.WriteLine($"offset {to} ещё не существует (последний — {high - 1})");
    return;
}

consumer.Assign(new TopicPartitionOffset(tp, from));
Console.WriteLine($"\n назначена партиция {partition}, старт с offset {from}\n");

int count = 0;
while (true)
{
    var r = consumer.Consume(TimeSpan.FromSeconds(5));

    if (r == null)
    {
        Console.WriteLine("Таймаут: за 5 секунд ничего не пришло");
        break;
    }

    if (r.Offset.Value > to) break;

    count++;

    var headers = r.Message.Headers == null
        ? ""
        : string.Join(", ", r.Message.Headers.Select(h =>
            $"{h.Key}={(h.GetValueBytes() is { } b ? Encoding.UTF8.GetString(b) : "null")}"));

    Console.WriteLine($"  offset {r.Offset.Value} | key={r.Message.Key} | headers: {headers} | {r.Message.Value}");

    if (r.Offset.Value == to) break; // дошли до конца диапазона
}

Console.WriteLine($"\nПрочитано сообщений: {count} (ожидалось {to - from + 1})");

consumer.Close(); // коммита НЕ будет мы ничего не помечали и автокоммит выключен

var after = GetCommitted(groupId, tp);
Console.WriteLine($"ПОСЛЕ offset группы '{groupId}': {after}"); // прочитали кусок, который группа уже читала, но закладка не должна поменяться
Console.WriteLine(before == after
    ? "Offset группы не изменился"
    : "Offset группы изменился");

static string GetCommitted(string groupId, TopicPartition tp)
{
    var cfg = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = groupId,
        EnableAutoCommit = false
    };
    using var c = new ConsumerBuilder<Ignore, Ignore>(cfg).Build();
    var committed = c.Committed(new[] { tp }, TimeSpan.FromSeconds(5));
    var offset = committed[0].Offset;
    return offset == Offset.Unset ? "нет коммита" : offset.Value.ToString();
}

/*
консьюмер:
партиция 0 | offset 0 | key=order-0 | eventType=OrderCreated | {"orderId":0,"amount":0}
партиция 0 | offset 1 | key=order-2 | eventType=OrderCreated | {"orderId":2,"amount":200}
партиция 0 | offset 2 | key=order-0 | eventType=OrderCreated | {"orderId":3,"amount":300}
партиция 0 | offset 3 | key=order-2 | eventType=OrderCreated | {"orderId":5,"amount":500}
партиция 0 | offset 4 | key=order-0 | eventType=OrderCreated | {"orderId":6,"amount":600}
партиция 0 | offset 5 | key=order-2 | eventType=OrderCreated | {"orderId":8,"amount":800}
партиция 0 | offset 6 | key=order-0 | eventType=OrderCreated | {"orderId":9,"amount":900}
партиция 1 | offset 0 | key=order-1 | eventType=OrderCreated | {"orderId":1,"amount":100}
партиция 1 | offset 1 | key=order-1 | eventType=OrderCreated | {"orderId":4,"amount":400}
партиция 1 | offset 2 | key=order-1 | eventType=OrderCreated | {"orderId":7,"amount":700}

планируем прочитать 2-5, то есть 
партиция 0 | offset 2 | key=order-0 | eventType=OrderCreated | {"orderId":3,"amount":300}
партиция 0 | offset 5 | key=order-2 | eventType=OrderCreated | {"orderId":8,"amount":800}
партиция 0 | offset 4 | key=order-0 | eventType=OrderCreated | {"orderId":6,"amount":600}
партиция 0 | offset 5 | key=order-2 | eventType=OrderCreated | {"orderId":8,"amount":800}

в 0 партиции 6 штук - high = 7(6+1)
ожидаю после отработки офсет там же на 7
*/