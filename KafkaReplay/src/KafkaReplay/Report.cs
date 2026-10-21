using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using Confluent.Kafka;
using MessageCheck;

namespace KafkaReplay;

public record ReportEntry(
    int Partition, long Offset, DateTimeOffset Timestamp, string TimestampType, bool OutsideWindow,
    string? Key, List<ReportHeader> Headers, string? Payload,
    string? PayloadSha256, string RecordSha256,
    string? EventType, string? PackageVersion, string? ContractVersion,
    string Verdict, string Reason, string? Details, string? ResultJson,
    List<ReportDifference> Differences, string? DifferencesNote,
    DeliveryStatus StatusCode, string Status, string? TargetTopic, int? TargetPartition)
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    // contractVersion — версия пакета контракта, которым читали (из сборки), packageVersion — из заголовка события
    public static ReportEntry From(ConsumeResult<byte[], byte[]> cr, KafkaRecord r, Decision d, bool outsideWindow, string? contractVersion)
    {
        var (diffs, note) = DifferencesOf(r, d);
        // статус до отправки
        var status = d.Verdict is MessageCheck.Verdict.Rejected or MessageCheck.Verdict.ConversionError
            ? DeliveryStatus.RejectedByCheck : DeliveryStatus.NotSent;

        return new(
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
            contractVersion,
            d.Verdict.ToString(), d.Reason.ToString(), d.Details, d.ResultJson,
            diffs, note,
            status, status.Ru(), null, null);
    }

    public ReportEntry WithStatus(DeliveryStatus status, string? targetTopic = null, int? targetPartition = null) =>
        this with { StatusCode = status, Status = status.Ru(), TargetTopic = targetTopic, TargetPartition = targetPartition };

    public bool IsRejected => string.Equals(Verdict, nameof(MessageCheck.Verdict.Rejected), StringComparison.Ordinal) || string.Equals(Verdict, nameof(MessageCheck.Verdict.ConversionError), StringComparison.Ordinal);
    public bool HasWarnings => Differences.Any(x => x.Warning);

    private static (List<ReportDifference>, string?) DifferencesOf(KafkaRecord r, Decision d)
    {
        if (d.Verdict != MessageCheck.Verdict.Allowed || d.ResultJson is null || r.Payload is null)
            return ([], $"сравнивать не с чем: событие не прочитано контрактом ({d.Reason})");
        try
        {
            var diffs = JsonDiff.Compare(r.Payload, d.ResultJson);
            return diffs.Count == 0
                ? ([], JsonDiff.NoDifferencesReason(r.Payload))
                : (diffs.Select(x => new ReportDifference(x.Path, x.Kind.ToString(), x.Describe(), x.IsWarning)).ToList(), null);
        }
        catch (System.Text.Json.JsonException)
        {
            return ([], "сравнить не удалось: тело не JSON-объект");
        }
    }

    private static string? FirstHeader(KafkaRecord r, string name) =>
        r.Headers.Where(h => string.Equals(h.Key, name, StringComparison.Ordinal) && h.Value is { Length: > 0 }).Select(h => Text(h.Value!)).FirstOrDefault();

    // текст, если это UTF-8, иначе hex
    private static string Text(byte[] bytes)
    {
        try { return StrictUtf8.GetString(bytes); }
        catch (DecoderFallbackException) { return $"<не UTF-8: {Convert.ToHexString(bytes)}>"; }
    }
}

public record ReportHeader(string Key, string? Value, string? Hex);

public record ReportDifference(string Path, string Kind, string Text, bool Warning);

public record SummaryRow(string EventType, string PackageVersion, string TargetTopic, string Status, int Count);

public static class ReportSummary
{
    public static List<SummaryRow> Rows(IEnumerable<ReportEntry> entries) =>
        entries.GroupBy(e => (Type: e.EventType ?? "—", Version: e.PackageVersion ?? "—", Target: e.TargetTopic ?? "—", e.Status))
               .Select(g => new SummaryRow(g.Key.Type, g.Key.Version, g.Key.Target, g.Key.Status, g.Count()))
               .OrderBy(r => r.EventType, StringComparer.Ordinal).ThenBy(r => r.PackageVersion, StringComparer.Ordinal).ThenBy(r => r.TargetTopic, StringComparer.Ordinal).ThenBy(r => r.Status, StringComparer.Ordinal)
               .ToList();

    public static Dictionary<string, int> ByStatus(IEnumerable<ReportEntry> entries) =>
        Enum.GetValues<DeliveryStatus>().ToDictionary(s => s.Ru(), s => entries.Count(e => e.StatusCode == s), StringComparer.Ordinal);
}

public static class ReportWriter
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Encoder = JavaScriptEncoder.Create(UnicodeRanges.All),
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
    };

    public static async Task<string> WriteAsync(string outDir, object report)
    {
        Directory.CreateDirectory(outDir);
        string path = Path.Combine(outDir, "report.json");
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(report, Options)).ConfigureAwait(false);
        return Path.GetFullPath(path);
    }
}
