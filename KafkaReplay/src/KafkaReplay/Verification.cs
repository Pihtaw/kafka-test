using Confluent.Kafka;
using MessageCheck;
using Header = MessageCheck.Header;

namespace KafkaReplay;

// что прочитали из целевого топика по адресу из подтверждения отправки
public record FetchedRecord(byte[]? Key, IReadOnlyList<Header> Headers, byte[]? Payload, long TimestampMs, string TimestampType);

public interface IRecordFetcher
{
    FetchedRecord? Fetch(string topic, int partition, long offset);
}

public static class ByteCompare
{
    public static VerifyResult Compare(PlanItem planned, FetchedRecord? actual)
    {
        if (actual is null) return new VerifyResult(planned.Index, false, ["записи по адресу из подтверждения нет"]);

        var expected = planned.ToRecord();
        var diffs = new List<string>();

        if (!Same(expected.Key, actual.Key)) diffs.Add("ключ отличается");
        if (!Same(expected.Payload, actual.Payload)) diffs.Add("тело отличается");

        if (expected.Headers.Count != actual.Headers.Count)
            diffs.Add($"заголовков {actual.Headers.Count}, ожидали {expected.Headers.Count}");
        else
            for (int i = 0; i < expected.Headers.Count; i++)
            {
                if (!string.Equals(expected.Headers[i].Key, actual.Headers[i].Key, StringComparison.Ordinal)) diffs.Add($"заголовок №{i + 1}: имя {actual.Headers[i].Key}, ожидали {expected.Headers[i].Key}");
                else if (!Same(expected.Headers[i].Value, actual.Headers[i].Value)) diffs.Add($"заголовок {expected.Headers[i].Key}: значение отличается");
            }

        bool matches = diffs.Count == 0;
        if (actual.TimestampMs != planned.TimestampMs)
            diffs.Add($"время {actual.TimestampMs} ({actual.TimestampType}), ожидали {planned.TimestampMs} — не влияет на совпадение");

        return new VerifyResult(planned.Index, matches, diffs);
    }

    private static bool Same(byte[]? a, byte[]? b) =>
        a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);
}

// Чтение одной записи по адресу (топик, партиция, offset) без группы и без коммита
public sealed class KafkaRecordFetcher(ClientConfig target) : IRecordFetcher, IDisposable
{
    private readonly IConsumer<byte[]?, byte[]?> _consumer = new ConsumerBuilder<byte[]?, byte[]?>(new ConsumerConfig(target)
    {
        GroupId = $"kafka-replay-verify-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}",
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false,
        AutoOffsetReset = AutoOffsetReset.Error
    }).Build();

    public FetchedRecord? Fetch(string topic, int partition, long offset)
    {
        _consumer.Assign(new TopicPartitionOffset(topic, partition, offset));
        var cr = _consumer.Consume(TimeSpan.FromSeconds(10));
        if (cr is null || cr.Offset.Value != offset) return null;

        var headers = cr.Message.Headers?.Select(h => new Header(h.Key, h.GetValueBytes())).ToList() ?? [];
        return new FetchedRecord(cr.Message.Key, headers, cr.Message.Value, cr.Message.Timestamp.UnixTimestampMs, cr.Message.Timestamp.Type.ToString());
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}
