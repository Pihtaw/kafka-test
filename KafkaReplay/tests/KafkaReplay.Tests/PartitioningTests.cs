using System.Text;
using KafkaReplay;

namespace KafkaReplay.Tests;

public class PartitioningTests
{
    [Fact]
    public void Crc32_matches_standard_check_value() =>
        Assert.Equal(0xCBF43926u, Partitioning.Crc32(Encoding.ASCII.GetBytes("123456789")));

    [Fact]
    public void Murmur2_matches_java_client_values()
    {
        (string key, uint expected)[] cases =
        [
            ("kafka", 0xd067cf64), ("giberish123456789", 0x8f552b0c), ("1234", 0x9fc97b14),
            ("234", 0xe7c009ca), ("34", 0x873930da), ("4", 0x5a4b5ca1), ("", 0x106e08d9),
        ];
        foreach (var (key, expected) in cases)
            Assert.Equal(expected, (uint)Partitioning.Murmur2(Encoding.UTF8.GetBytes(key)));
    }

    [Fact]
    public void Same_key_same_partition() =>
        Assert.Equal(
            Partitioning.Choose(PartitionStrategy.Crc32, "101"u8.ToArray(), 0, 3),
            Partitioning.Choose(PartitionStrategy.Crc32, "101"u8.ToArray(), 2, 3));

    [Fact]
    public void Crc32_and_murmur2_differ_for_known_key()
    {
        // из эксперимента: orderId 103 при 3 партициях — CRC32 даёт 0, murmur2 дает 2
        Assert.Equal(0, Partitioning.Choose(PartitionStrategy.Crc32, "103"u8.ToArray(), 0, 3));
        Assert.Equal(2, Partitioning.Choose(PartitionStrategy.Murmur2, "103"u8.ToArray(), 0, 3));
    }

    [Fact]
    public void Keep_uses_source_partition() =>
        Assert.Equal(2, Partitioning.Choose(PartitionStrategy.Keep, "101"u8.ToArray(), 2, 3));

    [Fact]
    public void Keep_wraps_when_target_has_fewer_partitions() =>
        Assert.Equal(1, Partitioning.Choose(PartitionStrategy.Keep, null, 5, 2));

    [Fact]
    public void Record_without_key_uses_source_partition() =>
        Assert.Equal(1, Partitioning.Choose(PartitionStrategy.Murmur2, null, 1, 3));
}
