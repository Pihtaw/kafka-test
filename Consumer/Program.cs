using System.Text;
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test-group",
    AutoOffsetReset = AutoOffsetReset.Earliest, // новая группа читает с начала
    EnableAutoCommit = true,                    // коммитим автоматически раз в 5 сек
    EnableAutoOffsetStore = false               // но только то что сами пометили как обработанное
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("test-topic");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

Console.WriteLine("Ждем");

try
{
    while (true)
    {
        var result = consumer.Consume(cts.Token);

        var eventType = result.Message.Headers.TryGetLastBytes("eventType", out var bytes)
            ? Encoding.UTF8.GetString(bytes)
            : "нет";

        Console.WriteLine(
            $"партиция {result.Partition.Value} | offset {result.Offset.Value} | " +
            $"key={result.Message.Key} | eventType={eventType} | {result.Message.Value}");

        consumer.StoreOffset(result); // обработали помечаем at least once ютуб
    }
}
catch (OperationCanceledException) { }
finally
{
    consumer.Close(); // корректно выйти из группы и закоммитить offset
}