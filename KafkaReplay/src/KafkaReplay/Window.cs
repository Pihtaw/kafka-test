namespace KafkaReplay;

// Где запись относительно окна по времени [from; to) и диапазона offsets [Start; End), который по этому окну дала Kafka.
// Kafka считает End как «первая запись со временем ≥ to». Если время в партиции идёт не по порядку,
// запись со временем внутри окна может лежать после End — поэтому после End просматриваем ещё «хвост».
public enum WindowPlace
{
    Inside,          // в диапазоне, время в окне — обычная запись
    OutsideWindow,   // в диапазоне, но время вне окна («чужое внутри»): проверяется, в план не идёт
    LateAfterEnd,    // после End, но время в окне («наше снаружи»): проверяется и идёт в план как обычная
    AfterEnd         // после End, время вне окна — не наше, пропускается
}

public static class Window
{
    public static WindowPlace Place(long offset, long end, long timestampMs, long fromMs, long toMs)
    {
        bool inTime = timestampMs >= fromMs && timestampMs < toMs;
        if (offset < end) return inTime ? WindowPlace.Inside : WindowPlace.OutsideWindow;
        return inTime ? WindowPlace.LateAfterEnd : WindowPlace.AfterEnd;
    }

    // до какого offset читать партицию: конец окна плюс хвост, но не дальше high
    public static long ReadTo(PartitionPlan p, long tail) =>
        tail >= p.High - p.End ? p.High : p.End + tail;
}

// что нашлось в хвосте после End, по партициям
public sealed class TailStats
{
    private readonly Dictionary<int, long> _late = [];
    private readonly Dictionary<int, long> _skipped = [];

    public void Add(int partition, WindowPlace place)
    {
        if (place == WindowPlace.LateAfterEnd) _late[partition] = _late.GetValueOrDefault(partition) + 1;
        else if (place == WindowPlace.AfterEnd) _skipped[partition] = _skipped.GetValueOrDefault(partition) + 1;
    }

    public long Late(int partition) => _late.GetValueOrDefault(partition);
    public long Skipped(int partition) => _skipped.GetValueOrDefault(partition);
    public long TotalLate => _late.Values.Sum();
    public long TotalSkipped => _skipped.Values.Sum();
}
