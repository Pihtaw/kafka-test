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
        Put(ms, r.Key);
        PutLength(ms, r.Headers.Count);
        foreach (var h in r.Headers)
        {
            Put(ms, Encoding.UTF8.GetBytes(h.Key));
            Put(ms, h.Value);
        }
        Put(ms, r.Payload);
        return Hex(SHA256.HashData(ms.ToArray()));
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
