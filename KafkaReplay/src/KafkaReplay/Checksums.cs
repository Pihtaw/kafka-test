using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using MessageCheck;

namespace KafkaReplay;

public static class Checksums
{
    // только тело события, без ключа и заголовков
    // у tombstone тела нет -> суммы нет.
    public static string? Payload(byte[]? payload) => payload is null ? null : Hex(SHA256.HashData(payload));

    // вся запись: ключ, заголовки по порядку, тело
    public static string Record(KafkaRecord r)
    {
        using var ms = new MemoryStream();
        PutRecord(ms, r);
        return Hex(SHA256.HashData(ms.ToArray()));
    }

    // строка плана отправки запись + куда и с каким временем она уйдёт
    public static string PlanItem(KafkaRecord r, string targetTopic, int targetPartition, long timestampMs)
    {
        using var ms = new MemoryStream();
        PutRecord(ms, r);
        Put(ms, Encoding.UTF8.GetBytes(targetTopic));
        PutLength(ms, targetPartition);
        Span<byte> ts = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(ts, timestampMs);
        ms.Write(ts);
        return Hex(SHA256.HashData(ms.ToArray()));
    }

    // весь план
    public static string Plan(IEnumerable<string> headerFields, IEnumerable<string> itemSums)
    {
        using var ms = new MemoryStream();
        foreach (var f in headerFields) Put(ms, Encoding.UTF8.GetBytes(f));
        foreach (var s in itemSums) Put(ms, Encoding.UTF8.GetBytes(s));
        return Hex(SHA256.HashData(ms.ToArray()));
    }

    private static void PutRecord(Stream ms, KafkaRecord r)
    {
        Put(ms, r.Key);
        PutLength(ms, r.Headers.Count);
        foreach (var h in r.Headers)
        {
            Put(ms, Encoding.UTF8.GetBytes(h.Key));
            Put(ms, h.Value);
        }
        Put(ms, r.Payload);
    }

    // длина перед каждым куском, чтобы исключить ["ab","c"] и ["a","bc"] -> в одну сумму
    // null пишем как длину -1, чтобы null и пустой массив тоже различались.
    private static void Put(Stream s, byte[]? bytes)
    {
        PutLength(s, bytes?.Length ?? -1);
        if (bytes is not null) s.Write(bytes);
    }

    private static void PutLength(Stream s, int length)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, length);
        s.Write(buf);
    }

    private static string Hex(byte[] hash) => Convert.ToHexString(hash).ToLowerInvariant();
}
