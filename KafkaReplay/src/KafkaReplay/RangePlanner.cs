using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaReplay;

// план чтения одной партиции: границы фиксируются до чтения
public record PartitionPlan(string Topic, int Partition, long Low, long High, long Start, long End);

// чтение диапазона по времени, как в RangeReader, только байты и без коммита
public static class RangePlanner
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    public static async Task<List<PartitionPlan>?> PlanAsync(ClientConfig connection, string topic, long fromMs, long toMs)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig(connection)).Build();
        var meta = admin.GetMetadata(topic, RequestTimeout).Topics[0];
        if (meta.Error.IsError) { Console.WriteLine($"ОТКАЗ: топик '{topic}' недоступен: {meta.Error.Reason}"); return null; }

        var partitions = meta.Partitions.Select(p => new TopicPartition(topic, p.PartitionId)).OrderBy(tp => tp.Partition.Value).ToList();

        async Task<Dictionary<int, long>> ListAsync(OffsetSpec spec)
        {
            var res = await admin.ListOffsetsAsync(
                partitions.Select(tp => new TopicPartitionOffsetSpec { TopicPartition = tp, OffsetSpec = spec }),
                new ListOffsetsOptions { RequestTimeout = RequestTimeout }).ConfigureAwait(false);
            return res.ResultInfos.ToDictionary(i => i.TopicPartitionOffsetError.Partition.Value, i => i.TopicPartitionOffsetError.Offset.Value);
        }

        var low = await ListAsync(OffsetSpec.Earliest()).ConfigureAwait(false);
        var high = await ListAsync(OffsetSpec.Latest()).ConfigureAwait(false);
        var from = await ListAsync(OffsetSpec.ForTimestamp(fromMs)).ConfigureAwait(false);
        var to = await ListAsync(OffsetSpec.ForTimestamp(toMs)).ConfigureAwait(false);

        return partitions.Select(tp =>
        {
            int p = tp.Partition.Value;
            long start = from[p] == Offset.End.Value ? high[p] : from[p];   // End → high: после T ничего нет
            long end = to[p] == Offset.End.Value ? high[p] : to[p];
            start = Math.Clamp(start, low[p], high[p]);
            end = Math.Clamp(end, start, high[p]);
            return new PartitionPlan(topic, p, low[p], high[p], start, end);
        }).ToList();
    }

    // без коммита, читаем [Start; End) через Assign
    public static IEnumerable<ConsumeResult<byte[], byte[]>> Read(ClientConfig connection, List<PartitionPlan> plan, ISet<int> finished)
    {
        foreach (var p in plan.Where(p => p.End <= p.Start)) finished.Add(p.Partition);
        var todo = plan.Where(p => p.End > p.Start).ToList();
        if (todo.Count == 0) yield break;

        var config = new ConsumerConfig(connection)
        {
            GroupId = $"kafka-replay-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}",
            AutoOffsetReset = AutoOffsetReset.Error,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true
        };
        using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        consumer.Assign(todo.Select(p => new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition), p.Start)));

        var endOf = todo.ToDictionary(p => p.Partition, p => p.End);
        var notDone = todo.Select(p => p.Partition).ToHashSet();

        while (notDone.Count > 0)
        {
            ConsumeResult<byte[], byte[]>? cr;
            try { cr = consumer.Consume(TimeSpan.FromSeconds(5)); }
            catch (ConsumeException e)
            {
                Console.WriteLine($"ОШИБКА чтения: {e.Error.Code} {e.Error.Reason}. Не дочитаны партиции {string.Join(", ", notDone)}");
                break;
            }
            if (cr is null) { Console.WriteLine($"Таймаут: не дочитаны партиции {string.Join(", ", notDone)}"); break; }

            int p = cr.Partition.Value;
            if (cr.IsPartitionEOF)
            {
                if (cr.Offset.Value >= endOf[p])
                {
                    Done(p, cr.TopicPartition);
                }
                continue;
            }
            if (cr.Offset.Value >= endOf[p]) { Done(p, cr.TopicPartition); continue; }
            yield return cr;
            if (cr.Offset.Value >= endOf[p] - 1) Done(p, cr.TopicPartition);
        }
        consumer.Close();

        void Done(int p, TopicPartition tp)
        {
            if (!notDone.Remove(p)) return;
            finished.Add(p);
            consumer.Pause([tp]);
        }
    }
}
