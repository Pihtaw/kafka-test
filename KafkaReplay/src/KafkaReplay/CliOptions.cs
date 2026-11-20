using System.Globalization;

namespace KafkaReplay;

public enum RunMode { DryRun, Execute }

// Параметры запуска после разбора командной строки (разбор — ReplayCli на System.CommandLine):
//   DryRun  (replay check):   прочитать, проверить, написать отчёт и план отправки
//   Execute (replay execute): отправить по сохранённому плану, проверить побайтово, написать статусы
public record CliOptions(
    RunMode Mode,
    string? Topic,
    DateTimeOffset From,
    DateTimeOffset To,
    string? Bootstrap,
    string? SourceEnv,
    string? Contracts,          // путь к сборке реестра контрактов
    string? Routes,             // таблица маршрутизации - нет - план не строится
    string? TargetBootstrap,
    string? TargetEnv,
    PartitionStrategy Partitioner,
    string? Plan,               // для execute: путь к plan.json
    string OutDir,
    long Tail = CliOptions.DefaultTail)   // сколько записей после конца окна просматривать в каждой партиции
{
    public const string TimeFormat = "yyyy-MM-dd HH:mm:sszzz";
    public const long DefaultTail = 10_000;

    public static bool TryParseTail(string s, out long tail)
    {
        if (string.Equals(s, "all", StringComparison.OrdinalIgnoreCase)) { tail = long.MaxValue; return true; }
        return long.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out tail);
    }
}
