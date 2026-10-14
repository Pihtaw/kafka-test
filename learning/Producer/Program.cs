using System.Text;
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All,   // ждём подтверждения
    LingerMs = 5       // шлём пачкой
};
// 1: при одних настройках партиционера сообщения без ключа раскидываются по партициям
// а при Partitioner.Consistent все попадают в одну партицию
/*var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Partitioner = Partitioner.Consistent, // удаляем норм 
    StickyPartitioningLingerMs = 0
};*/

using var producer = new ProducerBuilder<string, string>(config).Build();

for (int i = 0; i < 10; i++)
{
    var message = new Message<string, string>
    {
        //1: 
        //Key = null!,
        Key = $"order-{i % 3}",
        Value = $"{{\"orderId\":{i},\"amount\":{i * 100}}}",
        Headers = new Headers
        {
            { "eventType", Encoding.UTF8.GetBytes("OrderCreated") },
            { "version", Encoding.UTF8.GetBytes("1") }
        }
    };

    producer.Produce("test-topic", message, report =>
    {
        if (report.Error.IsError)
            Console.WriteLine($"ОШИБКА: {report.Error.Reason}");
        else
            Console.WriteLine($"Отправлено: key={report.Message.Key} -> партиция {report.Partition.Value}, offset {report.Offset.Value}");
    });
}

producer.Flush(TimeSpan.FromSeconds(10)); // дождаться пока всё уйдёт в Кафку
Console.WriteLine("Готово");