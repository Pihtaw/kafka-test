using System.Text.Json;
using Confluent.Kafka;

namespace KafkaReplay;

// результат отправки одной строки плана
public record SendOutcome(int Index, DeliveryStatus Status, int? Partition, long? Offset, string? Error);

public interface IPlanSender
{
    // отправить строки в порядке плана и вернуть статус каждой
    Task<IReadOnlyList<SendOutcome>> SendAsync(IReadOnlyList<PlanItem> items);
}

public record ItemState(int Index, string ItemSha256, DeliveryStatus Status, int? Partition, long? Offset, string? Error, DateTimeOffset At);

public record VerifyResult(int Index, bool Matches, List<string> Differences);

public record ExecutionFile(string PlanSha256, List<ItemState> Items, List<VerifyResult> Verification)
{
    public static string PathFor(string planPath) => Path.Combine(Path.GetDirectoryName(Path.GetFullPath(planPath))!, "execution.json");

    public static ExecutionFile? Load(string path) =>
        File.Exists(path) ? JsonSerializer.Deserialize<ExecutionFile>(File.ReadAllText(path), PlanStore.Options) : null;

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(this, PlanStore.Options));
}

public static class Executor
{
    public static List<PlanItem> ToSend(PlanFile plan, ExecutionFile? state)
    {
        var known = state?.Items.ToDictionary(s => s.Index) ?? [];
        return plan.Items.Where(i => !known.TryGetValue(i.Index, out var s) || s.Status == DeliveryStatus.NotWritten).ToList();
    }

    public static async Task<ExecutionFile> RunAsync(PlanFile plan, ExecutionFile? previous, IPlanSender sender, Action<ExecutionFile> save, DateTimeOffset now)
    {
        if (previous is not null && previous.PlanSha256 != plan.PlanSha256)
            throw new InvalidOperationException("execution.json относится к другому плану");

        var states = (previous?.Items ?? []).ToDictionary(s => s.Index);
        var batch = ToSend(plan, previous);

        foreach (var item in batch)
            states[item.Index] = new ItemState(item.Index, item.ItemSha256, DeliveryStatus.Unknown, null, null, "отправка начата, результата нет", now);
        save(Snapshot());

        var outcomes = batch.Count == 0 ? [] : await sender.SendAsync(batch);
        foreach (var o in outcomes)
            states[o.Index] = new ItemState(o.Index, plan.Items[o.Index].ItemSha256, o.Status, o.Partition, o.Offset, o.Error, now);

        var result = Snapshot();
        save(result);
        return result;

        ExecutionFile Snapshot() => new(plan.PlanSha256, states.Values.OrderBy(s => s.Index).ToList(), previous?.Verification ?? []);
    }
}

// отправка в Kafka: порядок внутри партиции сохраняется, повторы внутри
public sealed class KafkaPlanSender(ClientConfig target) : IPlanSender
{
    public async Task<IReadOnlyList<SendOutcome>> SendAsync(IReadOnlyList<PlanItem> items)
    {
        var config = new ProducerConfig(target)
        {
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 30_000,
            RequestTimeoutMs = 30_000
        };
        using var producer = new ProducerBuilder<byte[]?, byte[]?>(config).Build();

        var pending = new List<(PlanItem Item, Task<DeliveryResult<byte[]?, byte[]?>>? Task, SendOutcome? Failed)>();
        foreach (var item in items)
        {
            try
            {
                var r = item.ToRecord();
                var message = new Message<byte[]?, byte[]?>
                {
                    Key = r.Key,
                    Value = r.Payload,
                    Timestamp = new Timestamp(item.TimestampMs, TimestampType.CreateTime), // исходное время события
                    Headers = ToKafkaHeaders(r.Headers)
                };
                pending.Add((item, producer.ProduceAsync(new TopicPartition(item.TargetTopic, item.TargetPartition), message), null));
            }
            catch (Exception e) when (e is ProduceException<byte[]?, byte[]?> or KafkaException)
            {
                // не встало даже в очередь отправки — точно не записано
                pending.Add((item, null, new SendOutcome(item.Index, DeliveryStatus.NotWritten, null, null, e.Message)));
            }
        }

        var outcomes = new List<SendOutcome>();
        foreach (var (item, task, failed) in pending)
        {
            if (failed is not null) { outcomes.Add(failed); continue; }
            try
            {
                var dr = await task!;
                outcomes.Add(new SendOutcome(item.Index, Map(dr.Status), dr.Partition.Value, dr.Offset.Value, null));
            }
            catch (ProduceException<byte[]?, byte[]?> e)
            {
                outcomes.Add(new SendOutcome(item.Index, Map(e.DeliveryResult.Status), null, null, e.Error.Reason));
            }
            catch (Exception e)
            {
                // непонятно, дошло ли
                outcomes.Add(new SendOutcome(item.Index, DeliveryStatus.Unknown, null, null, e.Message));
            }
        }
        producer.Flush(TimeSpan.FromSeconds(10));
        return outcomes;
    }

    public static DeliveryStatus Map(PersistenceStatus s) => s switch
    {
        PersistenceStatus.Persisted => DeliveryStatus.Written,
        PersistenceStatus.NotPersisted => DeliveryStatus.NotWritten,
        _ => DeliveryStatus.Unknown
    };

    private static Headers ToKafkaHeaders(IReadOnlyList<MessageCheck.Header> headers)
    {
        var result = new Headers();
        foreach (var h in headers) result.Add(h.Key, h.Value); // в исходном порядке, повторы тоже
        return result;
    }
}
