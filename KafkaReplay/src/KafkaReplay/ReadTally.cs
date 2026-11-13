namespace KafkaReplay;

// Независимая сверка чтения. Записи считаются прямо в цикле чтения, до проверки и до отчёта,
// и сверяются с планом чтения. Тогда «сводка сходится» значит «ничего не потеряно, не прочитано дважды
// и не взято за границами плана», а не «список равен сам себе».
public sealed class ReadTally
{
    private readonly Dictionary<int, PartitionPlan> _plan;
    private readonly Dictionary<int, long> _end;   // до какого offset разрешено читать: End или End + хвост
    private readonly HashSet<(int Partition, long Offset)> _seen = [];
    private readonly Dictionary<int, long> _perPartition = [];

    public ReadTally(IEnumerable<PartitionPlan> plan, IReadOnlyDictionary<int, long>? readTo = null)
    {
        _plan = plan.ToDictionary(p => p.Partition);
        _end = _plan.Values.ToDictionary(p => p.Partition, p => readTo is not null && readTo.TryGetValue(p.Partition, out var e) ? e : p.End);
    }

    public long Consumed { get; private set; }     // сколько записей отдал цикл чтения
    public long Duplicates { get; private set; }   // та же партиция и offset второй раз
    public long OutOfRange { get; private set; }   // offset вне [Start; End + хвост) своей партиции или партиция не из плана

    public void Add(int partition, long offset)
    {
        Consumed++;
        if (!_seen.Add((partition, offset))) Duplicates++;
        if (!_plan.TryGetValue(partition, out var p) || offset < p.Start || offset >= _end[partition]) OutOfRange++;
        _perPartition[partition] = _perPartition.GetValueOrDefault(partition) + 1;
    }

    public long ReadIn(int partition) => _perPartition.GetValueOrDefault(partition);

    // прочитанное в цикле = записей в отчёте + пропущенных в хвосте, без повторов и без выхода за план
    public bool Matches(int reportEntries, long skippedInTail = 0) =>
        Consumed == reportEntries + skippedInTail && Duplicates == 0 && OutOfRange == 0;

    public string Describe(int reportEntries, long skippedInTail = 0) =>
        $"прочитано в цикле {Consumed}, записей в отчёте {reportEntries}" +
        (skippedInTail > 0 ? $", пропущено в хвосте (не наше время) {skippedInTail}" : "") +
        $", повторов {Duplicates}, вне плана {OutOfRange}";
}
