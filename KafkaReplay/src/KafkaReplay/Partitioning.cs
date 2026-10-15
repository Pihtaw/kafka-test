namespace KafkaReplay;

// Как выбрать партицию в целевом топике.
//   crc32   — как .NET-клиенты
//   murmur2 — как Java-клиент и Redpanda Connect
//   keep    — та же партиция, что в источнике
public enum PartitionStrategy { Crc32, Murmur2, Keep }

public static class Partitioning
{
    public static bool TryParse(string text, out PartitionStrategy strategy)
    {
        switch (text.ToLowerInvariant())
        {
            case "crc32": strategy = PartitionStrategy.Crc32; return true;
            case "murmur2": strategy = PartitionStrategy.Murmur2; return true;
            case "keep": strategy = PartitionStrategy.Keep; return true;
            default: strategy = default; return false;
        }
    }

    public static int Choose(PartitionStrategy strategy, byte[]? key, int sourcePartition, int targetPartitions)
    {
        if (targetPartitions <= 0) throw new ArgumentOutOfRangeException(nameof(targetPartitions));
        if (strategy == PartitionStrategy.Keep || key is null) return sourcePartition % targetPartitions;

        return strategy == PartitionStrategy.Crc32
            ? (int)(Crc32(key) % (uint)targetPartitions)
            : (Murmur2(key) & 0x7fffffff) % targetPartitions;
    }

    // далее реализация из первых комитов
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint n = 0; n < 256; n++)
        {
            uint c = n;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[n] = c;
        }
        return table;
    }

    public static uint Crc32(byte[] data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data) crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }

    // murmur2
    public static int Murmur2(byte[] data)
    {
        const uint m = 0x5bd1e995;
        const int r = 24;
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

        h ^= h >> 13; h *= m; h ^= h >> 15;
        return (int)h;
    }
}
