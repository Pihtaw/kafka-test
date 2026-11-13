namespace KafkaReplay;

// Независимая сверка чтения. Записи считаются прямо в цикле чтения, до проверки и до отчёта,
// и сверяются с планом чтения. Тогда «сводка сходится» значит «ничего не потеряно, не прочитано дважды
// и не взято за границами плана», а не «список равен сам себе».
public sealed class ReadTally
{
    private readonly Dictionary<int, PartitionPlan> _plan;
    private readonly HashSet<(int Partition, long Offset)> _seen = [];
    private readonly Dictionary<int, long> _perPartition = [];

    public ReadTally(IEnumerable<PartitionPlan> plan) => _plan = plan.ToDictionary(p => p.Partition);

    public long Consumed { get; private set; }     // сколько записей отдал цикл чтения
    public long Duplicates { get; private set; }   // та же партиция и offset второй раз
    public long OutOfRange { get; private set; }   // offset вне [Start; End) своей партиции или партиция не из плана

    public void Add(int partition, long offset)
    {
        Consumed++;
        if (!_seen.Add((partition, offset))) Duplicates++;
        if (!_plan.TryGetValue(partition, out var p) || offset < p.Start || offset >= p.End) OutOfRange++;
        _perPartition[partition] = _perPartition.GetValueOrDefault(partition) + 1;
    }

    public long ReadIn(int partition) => _perPartition.GetValueOrDefault(partition);

    // прочитанное в цикле = записей в отчёте, без повторов и без выхода за план
    public bool Matches(int reportEntries) => Consumed == reportEntries && Duplicates == 0 && OutOfRange == 0;

    public string Describe(int reportEntries) =>
        $"прочитано в цикле {Consumed}, записей в отчёте {reportEntries}, повторов {Duplicates}, вне плана {OutOfRange}";
}
