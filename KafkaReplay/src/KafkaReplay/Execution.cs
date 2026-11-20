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

// NotVerified — сверка не состоялась (ошибка чтения цели), причина; это не «расхождение», повторный execute сверит заново
public record VerifyResult(int Index, bool Matches, List<string> Differences, string? NotVerified = null)
{
    public static VerifyResult Failed(int index, string reason) => new(index, false, [], reason);
}

public record ExecutionFile(string PlanSha256, List<ItemState> Items, List<VerifyResult> Verification)
{
    public static string PathFor(string planPath) => Path.Combine(Path.GetDirectoryName(Path.GetFullPath(planPath))!, "execution.json");

    public static ExecutionFile? Load(string path) =>
        File.Exists(path) ? JsonSerializer.Deserialize<ExecutionFile>(File.ReadAllText(path), PlanStore.Options) : null;

    // пишется после каждой пачки — поэтому атомарно: оборванная запись не оставит полфайла
    public void Save(string path) => AtomicFile.WriteAllText(path, JsonSerializer.Serialize(this, PlanStore.Options));
}

public static class Executor
{
    public static List<PlanItem> ToSend(PlanFile plan, ExecutionFile? state)
    {
        var known = state?.Items.ToDictionary(s => s.Index) ?? [];
        // «не отправлялось» — пачки после остановки на ошибке; «результат неизвестен» не повторяем никогда
        return plan.Items.Where(i => !known.TryGetValue(i.Index, out var s) || s.Status is DeliveryStatus.NotWritten or DeliveryStatus.NotSent).ToList();
    }

    // Пачками: перед отправкой пачка помечается «результат неизвестен» и сохраняется, после ответа — сохраняются статусы.
    // Если в пачке не всё «записано», дальше не идём: иначе следующие записи той же партиции встали бы в цель раньше
    // незаписанной, и порядок сломался бы. Остальные строки — «не отправлялось», повторный запуск продолжит с этого места
    public static async Task<ExecutionFile> RunAsync(PlanFile plan, ExecutionFile? previous, IPlanSender sender, Action<ExecutionFile> save, DateTimeOffset now,
                                                     int batchSize = KafkaPlanSender.BatchSize)
    {
        if (previous is not null && !string.Equals(previous.PlanSha256, plan.PlanSha256, StringComparison.Ordinal))
            throw new InvalidOperationException("execution.json относится к другому плану");

        var states = (previous?.Items ?? []).ToDictionary(s => s.Index);
        var batches = ToSend(plan, previous).Chunk(batchSize).ToList();
        int total = batches.Sum(b => b.Length);
        int done = 0;

        for (int b = 0; b < batches.Count; b++)
        {
            foreach (var item in batches[b])
                states[item.Index] = new ItemState(item.Index, item.ItemSha256, DeliveryStatus.Unknown, null, null, "отправка начата, результата нет", now);
            save(Snapshot());

            var outcomes = await sender.SendAsync(batches[b]).ConfigureAwait(false);
            foreach (var o in outcomes)
                states[o.Index] = new ItemState(o.Index, plan.Items[o.Index].ItemSha256, o.Status, o.Partition, o.Offset, o.Error, now);
            save(Snapshot());
            done += batches[b].Length;
            Console.WriteLine($"  отправлено {done} из {total}");

            if (outcomes.All(o => o.Status == DeliveryStatus.Written)) continue;

            var rest = batches.Skip(b + 1).SelectMany(x => x).ToList();
            foreach (var item in rest)
                states[item.Index] = new ItemState(item.Index, item.ItemSha256, DeliveryStatus.NotSent, null, null, "не отправлялось: отправка остановлена после ошибки в предыдущей пачке", now);
            if (rest.Count > 0)
            {
                save(Snapshot());
                Console.WriteLine($"  ⚠ в пачке не всё записано — отправка остановлена, не отправлено {rest.Count}; повторный execute продолжит с этого места");
            }
            break;
        }

        var result = Snapshot();
        save(result);
        return result;

        ExecutionFile Snapshot() => new(plan.PlanSha256, states.Values.OrderBy(s => s.Index).ToList(), previous?.Verification ?? []);
    }
}

// отправка в Kafka: один продюсер на весь запуск, порядок внутри партиции сохраняется, повторы внутри
public sealed class KafkaPlanSender : IPlanSender, IDisposable
{
    // Пачками (пачки нарезает Executor): следующая пачка ставится в очередь, только когда по предыдущей пришли ответы.
    // Иначе на медленном канале (port-forward) тысячи сообщений ждут в очереди дольше таймаута и «теряются»,
    // хотя часть из них брокер уже записал
    public const int BatchSize = 200;

    private readonly IProducer<byte[]?, byte[]?> _producer;

    public KafkaPlanSender(ClientConfig target)
    {
        // копия: ProducerConfig(ClientConfig) меняет переданный объект на месте, и его настройки утекли бы в консьюмер сверки
        var config = new ProducerConfig(target.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal))
        {
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 120_000,
            RequestTimeoutMs = 30_000,
            AllowAutoCreateTopics = false,
        };
        _producer = new ProducerBuilder<byte[]?, byte[]?>(config).Build();
    }

    public Task<IReadOnlyList<SendOutcome>> SendAsync(IReadOnlyList<PlanItem> items) => SendBatchAsync(_producer, items);

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }

    // Ошибка может прийти двумя путями, и оба обработаны:
    //   1) синхронно, пока сообщение собирается и ставится в очередь, — ловит первый catch: точно не записано;
    //   2) через таску — ProduceAsync async-метод, поэтому всё, что случилось внутри него (и до, и после первой
    //      асинхронной операции), выбрасывается на await ниже: там ProduceException → по ответу брокера, остальное → неизвестно.
    // Сразу await на каждое сообщение не делаем: тогда отправка шла бы по одному сообщению за круг к брокеру
    private static async Task<IReadOnlyList<SendOutcome>> SendBatchAsync(IProducer<byte[]?, byte[]?> producer, IReadOnlyList<PlanItem> batch)
    {
        var pending = new List<(PlanItem Item, Task<DeliveryResult<byte[]?, byte[]?>>? Task, SendOutcome? Failed)>();
        foreach (var item in batch)
        {
            try
            {
                var r = item.ToRecord();
                var message = new Message<byte[]?, byte[]?>
                {
                    Key = r.Key,
                    Value = r.Payload,
                    // время записи не задаём — продюсер ставит текущее: в цели это новая запись, и retention
                    // считается от переналивки, а не от исходного времени. Время самого события не теряется:
                    // оно в заголовке Event-Timestamp и в теле, а они переносятся байт в байт
                    Headers = ToKafkaHeaders(r.Headers)
                };
                pending.Add((item, producer.ProduceAsync(new TopicPartition(item.TargetTopic, item.TargetPartition), message), null));
            }
            catch (Exception e)
            {
                // не встало даже в очередь отправки — точно не записано
                pending.Add((item, null, new SendOutcome(item.Index, DeliveryStatus.NotWritten, null, null, e.Message)));
            }
        }

        var outcomes = new List<SendOutcome>(batch.Count);
        foreach (var (item, task, failed) in pending)
        {
            if (failed is not null) { outcomes.Add(failed); continue; }
            try
            {
                var dr = await task!.ConfigureAwait(false);
                outcomes.Add(new SendOutcome(item.Index, Map(dr.Status), dr.Partition.Value, dr.Offset.Value, null));
            }
            catch (ProduceException<byte[]?, byte[]?> e)
            {
                outcomes.Add(new SendOutcome(item.Index, StatusOnError(e.Error.IsLocalError, e.DeliveryResult.Status), null, null, e.Error.Reason));
            }
            catch (Exception e)
            {
                // непонятно, дошло ли
                outcomes.Add(new SendOutcome(item.Index, DeliveryStatus.Unknown, null, null, e.Message));
            }
        }
        return outcomes;
    }

    public static DeliveryStatus Map(PersistenceStatus s) => s switch
    {
        PersistenceStatus.Persisted => DeliveryStatus.Written,
        PersistenceStatus.NotPersisted => DeliveryStatus.NotWritten,
        _ => DeliveryStatus.Unknown
    };

    // Ошибка после того, как сообщение встало в очередь. «Точно не записано» — только если ответил брокер
    // и сказал «не записал» (например, слишком большое сообщение). Ошибка на нашей стороне (таймаут, обрыв связи) —
    // «результат неизвестен»: сообщение могло уже уйти, и брокер мог его записать.
    // Проверено на dev: 93 сообщения с «Local: Message timed out» и статусом NotPersisted оказались в топике
    public static DeliveryStatus StatusOnError(bool isLocalError, PersistenceStatus status) =>
        !isLocalError && status == PersistenceStatus.NotPersisted ? DeliveryStatus.NotWritten : DeliveryStatus.Unknown;

    private static Headers ToKafkaHeaders(IReadOnlyList<MessageCheck.Header> headers)
    {
        var result = new Headers();
        foreach (var h in headers) result.Add(h.Key, h.Value); // в исходном порядке, повторы тоже
        return result;
    }
}
