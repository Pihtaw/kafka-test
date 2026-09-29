using Confluent.Kafka;

var mode = args.Length > 0 ? args[0] : "";
if (mode != "before" && mode != "after")
{
    Console.WriteLine("Укажи режим: dotnet run -- after  или  dotnet run -- before");
    return;
}

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = $"crash-test-{mode}-v2",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
    EnableAutoOffsetStore = false
};
Console.WriteLine($"Режим: {mode}, группа: {config.GroupId}");

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("test-topic");

try
{
    while (true)
    {
        var r = consumer.Consume(TimeSpan.FromSeconds(15));
        if (r == null) { Console.WriteLine("Новых сообщений нет"); break; }

        if (mode == "before") consumer.StoreOffset(r); // пометили ДО обработки

        Process(r);

        if (mode == "after") consumer.StoreOffset(r);  // пометили ПОСЛЕ обработки
    }
}
catch (Exception ex)
{
    Console.WriteLine($"!!! {ex.Message}");   // ловим падение
}
finally
{
    consumer.Close();  // выйти из группы и закоммитить помеченные offsets
    Console.WriteLine("Консьюмер корректно вышел из группы");
}

void Process(ConsumeResult<string, string> r)
{
    if (r.Message.Value.Contains("\"orderId\":5,") && !File.Exists("fixed.txt"))
        throw new Exception($"УПАЛИ на offset {r.Offset.Value} (orderId 5)");

    Console.WriteLine($"Обработано: p{r.Partition.Value} o{r.Offset.Value} {r.Message.Value}");
}