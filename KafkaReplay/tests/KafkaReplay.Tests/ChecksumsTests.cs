using KafkaReplay;
using MessageCheck;

namespace KafkaReplay.Tests;

public class ChecksumsTests
{
    private static KafkaRecord Record(string? key, string? payload, params (string Key, string Value)[] headers) => new(
        headers.Select(h => new Header(h.Key, System.Text.Encoding.UTF8.GetBytes(h.Value))).ToList(),
        key is null ? null : System.Text.Encoding.UTF8.GetBytes(key),
        payload is null ? null : System.Text.Encoding.UTF8.GetBytes(payload));

    [Fact]
    public void Payload_hash_is_plain_sha256_of_body() =>
        // sha256 пустой строки
        Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Checksums.Payload([]));

    [Fact]
    public void Tombstone_has_no_payload_hash() =>
        Assert.Null(Checksums.Payload(null));

    [Fact]
    public void Header_change_changes_record_hash_but_not_payload_hash()
    {
        var a = Record("101", """{"OrderId":101}""", ("Event-Type", "A"));
        var b = Record("101", """{"OrderId":101}""", ("Event-Type", "B"));

        Assert.Equal(Checksums.Payload(a.Payload), Checksums.Payload(b.Payload));
        Assert.NotEqual(Checksums.Record(a), Checksums.Record(b));
    }

    [Fact]
    public void Key_change_changes_record_hash() =>
        Assert.NotEqual(Checksums.Record(Record("101", "{}")), Checksums.Record(Record("102", "{}")));

    [Fact]
    public void Header_order_matters() =>
        Assert.NotEqual(
            Checksums.Record(Record("k", "{}", ("a", "1"), ("b", "2"))),
            Checksums.Record(Record("k", "{}", ("b", "2"), ("a", "1"))));

    [Fact]
    public void Null_and_empty_differ() =>
        Assert.NotEqual(Checksums.Record(Record(null, "{}")), Checksums.Record(Record("", "{}")));

    [Fact]
    public void Moving_bytes_between_parts_changes_hash() =>
        // без длин "ab"+"c" и "a"+"bc" склеились бы одинаково
        Assert.NotEqual(Checksums.Record(Record("ab", "c")), Checksums.Record(Record("a", "bc")));

    [Fact]
    public void Same_record_gives_same_hash() =>
        Assert.Equal(
            Checksums.Record(Record("101", """{"OrderId":101}""", ("Event-Type", "A"))),
            Checksums.Record(Record("101", """{"OrderId":101}""", ("Event-Type", "A"))));
}
