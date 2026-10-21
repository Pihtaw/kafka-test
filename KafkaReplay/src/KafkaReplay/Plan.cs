using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;
using MessageCheck;

namespace KafkaReplay;

// Пять статусов записи
public enum DeliveryStatus
{
    Written,         // записано
    NotWritten,      // точно не записано
    Unknown,         // результат неизвестен
    RejectedByCheck, // отклонено проверкой
    NotSent          // не отправлялось: чужое событие, tombstone, нет маршрута, или запуск без --execute
}

public static class DeliveryStatusText
{
    public static string Ru(this DeliveryStatus s) => s switch
    {
        DeliveryStatus.Written => "записано",
        DeliveryStatus.NotWritten => "точно не записано",
        DeliveryStatus.Unknown => "результат неизвестен",
        DeliveryStatus.RejectedByCheck => "отклонено проверкой",
        DeliveryStatus.NotSent => "не отправлялось",
        _ => s.ToString()
    };
}

public record PlanHeader(string Key, string? Value);

public record PlanItem(
    int Index,
    int SourcePartition, long SourceOffset, long TimestampMs, string EventType,
    string? Key, List<PlanHeader> Headers, string? Payload,
    string TargetTopic, int TargetPartition,
    string ItemSha256)
{
    public static PlanItem Create(int index, int sourcePartition, long sourceOffset, long timestampMs, string eventType,
                                  KafkaRecord r, string targetTopic, int targetPartition) => new(
        index, sourcePartition, sourceOffset, timestampMs, eventType,
        B64(r.Key), r.Headers.Select(h => new PlanHeader(h.Key, B64(h.Value))).ToList(), B64(r.Payload),
        targetTopic, targetPartition,
        Checksums.PlanItem(r, targetTopic, targetPartition, timestampMs));

    public KafkaRecord ToRecord() => new(
        Headers.Select(h => new Header(h.Key, Bytes(h.Value))).ToList(), Bytes(Key), Bytes(Payload));

    private static string? B64(byte[]? b) => b is null ? null : Convert.ToBase64String(b);
    private static byte[]? Bytes(string? s) => s is null ? null : Convert.FromBase64String(s);
}

// план отправки
public record PlanFile(
    int FormatVersion,
    DateTimeOffset CreatedAt,
    string SourceTopic,
    string SourceDescription,
    string SourceBootstrap,
    string TargetDescription,
    string TargetBootstrap,
    string Partitioner,
    string? Contracts,
    int TotalRecords,                                   // сколько записей прочитано всего
    Dictionary<string, int> NotInPlan,                  // статусы записей, которые в план не попали
    List<PlanItem> Items,
    string PlanSha256)
{
    public const int CurrentFormat = 1;

    public static PlanFile Create(DateTimeOffset createdAt, string sourceTopic, string sourceDescription, string sourceBootstrap,
                                  string targetDescription, string targetBootstrap, string partitioner, string? contracts,
                                  int totalRecords, Dictionary<string, int> notInPlan, List<PlanItem> items)
    {
        var draft = new PlanFile(CurrentFormat, createdAt, sourceTopic, sourceDescription, sourceBootstrap,
            targetDescription, targetBootstrap, partitioner, contracts, totalRecords, notInPlan, items, "");
        return draft with { PlanSha256 = draft.ComputeSha256() };
    }

    // подмена цели или партиционера в файле должна блокировать отправку
    public string ComputeSha256() => Checksums.Plan(
        [FormatVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), CreatedAt.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture), SourceTopic, SourceBootstrap, TargetBootstrap,
         Partitioner, Contracts ?? "", TotalRecords.ToString(System.Globalization.CultureInfo.InvariantCulture),
         string.Join(";", NotInPlan.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => $"{x.Key}={x.Value}"))],
        Items.Select(i => i.ItemSha256));

    // пересчитать сумму каждой строки и всего плана
    public List<string> Verify()
    {
        var problems = new List<string>();
        if (FormatVersion != CurrentFormat) problems.Add($"формат плана {FormatVersion}, утилита понимает {CurrentFormat}");

        for (int i = 0; i < Items.Count; i++)
        {
            var item = Items[i];
            if (item.Index != i) problems.Add($"строка {i}: номер {item.Index} — порядок строк нарушен");
            try
            {
                string actual = Checksums.PlanItem(item.ToRecord(), item.TargetTopic, item.TargetPartition, item.TimestampMs);
                if (!string.Equals(actual, item.ItemSha256, StringComparison.Ordinal)) problems.Add($"строка {i} (p{item.SourcePartition} o{item.SourceOffset}): сумма не совпадает");
            }
            catch (FormatException)
            {
                problems.Add($"строка {i} (p{item.SourcePartition} o{item.SourceOffset}): испорчен base64");
            }
        }

        if (!string.Equals(ComputeSha256(), PlanSha256, StringComparison.Ordinal)) problems.Add("сумма всего плана не совпадает (изменена шапка, порядок или состав строк)");
        return problems;
    }
}

public static class PlanStore
{
    public static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Encoder = JavaScriptEncoder.Create(UnicodeRanges.All),
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task<string> SaveAsync(string outDir, PlanFile plan)
    {
        Directory.CreateDirectory(outDir);
        string path = Path.Combine(outDir, "plan.json");
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(plan, Options)).ConfigureAwait(false);
        return Path.GetFullPath(path);
    }

    public static bool TryLoad(string path, out PlanFile? plan, out string? error)
    {
        plan = null;
        try
        {
            plan = JsonSerializer.Deserialize<PlanFile>(File.ReadAllText(path), Options);
            if (plan?.Items is null) { error = $"{path}: это не план отправки"; plan = null; return false; }
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or JsonException)
        {
            error = $"не удалось прочитать план {path}: {e.Message}";
            return false;
        }
        error = null;
        return true;
    }
}
