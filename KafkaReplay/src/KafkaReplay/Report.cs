using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using Confluent.Kafka;
using MessageCheck;

namespace KafkaReplay;

// Строка отчёта: одна запись из топика и решение по ней
public record ReportEntry(
    int Partition, long Offset, DateTimeOffset Timestamp, string TimestampType, bool OutsideWindow,
    string? Key, List<ReportHeader> Headers, string? Payload,
    string? PayloadSha256, string RecordSha256,
    string? EventType, string? PackageVersion, string Verdict, string Reason, string? Details, string? ResultJson)
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static ReportEntry From(ConsumeResult<byte[], byte[]> cr, KafkaRecord r, Decision d, bool outsideWindow) => new(
        cr.Partition.Value, cr.Offset.Value,
        DateTimeOffset.FromUnixTimeMilliseconds(cr.Message.Timestamp.UnixTimestampMs).ToLocalTime(),
        cr.Message.Timestamp.Type.ToString(), outsideWindow,
        r.Key is null ? null : Text(r.Key),
        r.Headers.Select(h => new ReportHeader(h.Key, h.Value is null ? null : Text(h.Value), h.Value is null ? null : Convert.ToHexString(h.Value))).ToList(),
        r.Payload is null ? null : Text(r.Payload),
        Checksums.Payload(r.Payload),
        Checksums.Record(r),
        FirstHeader(r, HeaderNames.EventType),
        FirstHeader(r, HeaderNames.PackageVersion),
        d.Verdict.ToString(), d.Reason.ToString(), d.Details, d.ResultJson);

    public bool IsRejected => Verdict == nameof(MessageCheck.Verdict.Rejected) || Verdict == nameof(MessageCheck.Verdict.ConversionError);

    private static string? FirstHeader(KafkaRecord r, string name) =>
        r.Headers.Where(h => h.Key == name && h.Value is { Length: > 0 }).Select(h => Text(h.Value!)).FirstOrDefault();

    // текст, если это UTF-8, иначе hex
    private static string Text(byte[] bytes)
    {
        try { return StrictUtf8.GetString(bytes); }
        catch (DecoderFallbackException) { return $"<не UTF-8: {Convert.ToHexString(bytes)}>"; }
    }
}

public record ReportHeader(string Key, string? Value, string? Hex);

public static class ReportWriter
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
    };

    public static async Task<string> WriteAsync(string outDir, object report)
    {
        Directory.CreateDirectory(outDir);
        string path = Path.Combine(outDir, "report.json");
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(report, Options));
        return Path.GetFullPath(path);
    }
}
