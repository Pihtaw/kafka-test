namespace KafkaReplay;

// коды выхода: как в репозитории
public static class ExitCodes
{
    public const int Ok = 0;           // всё прочитано, сводка сходится, отказов нет
    public const int HasRejected = 1;  // отработала, но есть записи, отклонённые проверкой
    public const int Incomplete = 2;   // не дочитали диапазон или сводка не сходится
    public const int Blocked = 3;      // ошибка проверки, не запустилась: параметры, файл подключения, нет связи с Kafka

    public static int For(bool allRead, bool sumMatches, int rejected)
    {
        if (!allRead || !sumMatches) return Incomplete;
        if (rejected > 0) return HasRejected;
        return Ok;
    }

    public static string Describe(int code) => code switch
    {
        Ok => "всё прочитано, отказов нет",
        HasRejected => "есть записи, отклонённые проверкой",
        Incomplete => "диапазон дочитан не полностью или сводка не сходится",
        Blocked => "не запустилась",
        _ => "неизвестный код"
    };
}
