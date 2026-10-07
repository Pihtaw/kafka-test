using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092";
var mode = args.Length > 0 ? args[0] : "";

return mode switch
{
    "hash" => await HashExperiment.RunAsync(bootstrap, args.Length > 1 ? int.Parse(args[1]) : 3),
    "time" => await TimeExperiment.RunAsync(bootstrap,
                  producers: args.Length > 1 ? int.Parse(args[1]) : 4,
                  perProducer: args.Length > 2 ? int.Parse(args[2]) : 2000,
                  skewMs: args.Length > 3 ? int.Parse(args[3]) : 0),
    _ => Usage()
};

static int Usage()
{
    Console.WriteLine("dotnet run -- hash [партиций]");
    Console.WriteLine("dotnet run -- time [продюсеров] [сообщений на продюсера] [сдвиг часов первого продюсера, мс]");
    return 1;
}

// ТЕСТ1: один и тот же ключ (у нас orderId) через разные партиционеры
static class HashExperiment
{
    public static async Task<int> RunAsync(string bootstrap, int partitions)
    {
        string topic = $"hash-test-{partitions}";
        await Topics.EnsureAsync(bootstrap, topic, partitions);

        // librdkafka: consistent_random = crc32(ключ) % партиций
        using var dotnetDefault = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();
        // Murmur2 (Java и Redpanda Connect)
        using var dotnetMurmur = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap, Partitioner = Partitioner.Murmur2 }).Build();

        var keys = Enumerable.Range(1, 20).Select(i => (100 + i).ToString()).ToList();

        Console.WriteLine($"Топик {topic}, партиций: {partitions}\n");
        Console.WriteLine("orderId | .NET дефолт | .NET Murmur2 | CRC32 % n | murmur2 % n | совпали?");
        int differ = 0;
        foreach (var key in keys)
        {
            var a = await dotnetDefault.ProduceAsync(topic, new Message<string, string> { Key = key, Value = "default" });
            var b = await dotnetMurmur.ProduceAsync(topic, new Message<string, string> { Key = key, Value = "murmur2" });

            var bytes = Encoding.UTF8.GetBytes(key);
            long crc = Hashes.Crc32(bytes) % (uint)partitions;
            long mur = (Hashes.Murmur2(bytes) & 0x7fffffff) % partitions;

            bool same = a.Partition.Value == b.Partition.Value;
            if (!same) differ++;
            Console.WriteLine($"{key,7} | {a.Partition.Value,11} | {b.Partition.Value,12} | {crc,9} | {mur,11} | {(same ? "да" : "НЕТ")}");
        }

        Console.WriteLine($"\nОдин и тот же orderId попал в РАЗНЫЕ партиции у {differ} из {keys.Count} ключей.");
        return 0;
    }
}

// 2. ТЕСТ2: несколько продюсеров параллельно пишут в одну партицию
static class TimeExperiment
{
    public static async Task<int> RunAsync(string bootstrap, int producers, int perProducer, int skewMs)
    {
        const string topic = "time-test";
        await Topics.EnsureAsync(bootstrap, topic, 1);
        var tp = new TopicPartition(topic, 0);

        // где кончалась партиция _до_ эксперимента
        long startOffset;
        using (var probe = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "time-test-probe" }).Build())
            startOffset = probe.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)).High.Value;

        Console.WriteLine($"{producers} продюсеров × {perProducer} сообщений → {topic}" +
                          (skewMs != 0 ? $", у продюсера 0 часы отстают на {skewMs} мс" : ""));

        // каждый продюсер — свой экземпляр клиента, как разные сервисы
        // LingerMs копит пачки, которые перемешиваются в партиции
        var tasks = Enumerable.Range(0, producers).Select(id => Task.Run(() =>
        {
            using var p = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap, LingerMs = 20 }).Build();
            for (int i = 0; i < perProducer; i++)
            {
                var msg = new Message<Null, string> { Value = $"p{id}-{i}" };
                if (id == 0 && skewMs != 0)
                    msg.Timestamp = new Timestamp(DateTimeOffset.UtcNow.AddMilliseconds(-skewMs)); // отстающие часы отправителя
                p.Produce(topic, msg);
            }
            p.Flush(TimeSpan.FromSeconds(30));
        })).ToArray();
        await Task.WhenAll(tasks);

        // смотрим идет ли время назад
        using var c = new ConsumerBuilder<Ignore, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "time-test-reader", EnableAutoCommit = false }).Build();
        long end = c.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)).High.Value;
        c.Assign(new TopicPartitionOffset(tp, startOffset));

        long prev = long.MinValue, read = 0, backward = 0, maxBack = 0;
        while (read < end - startOffset)
        {
            var r = c.Consume(TimeSpan.FromSeconds(5));
            if (r is null) break;
            read++;
            long ts = r.Message.Timestamp.UnixTimestampMs;
            if (ts < prev) { backward++; maxBack = Math.Max(maxBack, prev - ts); }
            prev = Math.Max(prev, ts);
        }
        c.Close();

        Console.WriteLine($"\nПрочитано {read} сообщений (offsets {startOffset}..{end - 1})");
        Console.WriteLine($"Время шло назад у {backward} сообщений ({100.0 * backward / Math.Max(read, 1):F2}%)");
        Console.WriteLine($"Максимальный откат: {maxBack} мс");
        Console.WriteLine(backward == 0
            ? "На этих данных окно по времени = окно по offsets."
            : "Окно с–по по offsets может захватить сообщение со временем вне окна.");
        return 0;
    }
}

static class Topics
{
    public static async Task EnsureAsync(string bootstrap, string topic, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try { await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]); }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)) { }
    }
}

static class Hashes
{
    // crc32 (готовый из System.IO.Hashing: librdkafka)
    public static uint Crc32(byte[] data) => System.IO.Hashing.Crc32.HashToUInt32(data);

    // murmur2 (нет готового на net, украдено из https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L502-L541)
    public static int Murmur2(byte[] data)
    {
        const uint m = 0x5bd1e995; const int r = 24;
        int length = data.Length;
        uint h = 0x9747b28c ^ (uint)length;
        int i = 0;
        for (; i + 4 <= length; i += 4)
        {
            uint k = (uint)(data[i] | data[i + 1] << 8 | data[i + 2] << 16 | data[i + 3] << 24);
            k *= m; k ^= k >> r; k *= m;
            h *= m; h ^= k;
        }
        switch (length % 4)
        {
            case 3: h ^= (uint)data[i + 2] << 16; goto case 2;
            case 2: h ^= (uint)data[i + 1] << 8; goto case 1;
            case 1: h ^= data[i]; h *= m; break;
        }
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;
        return (int)h;
    }
}
