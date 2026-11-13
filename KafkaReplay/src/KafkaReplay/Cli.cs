namespace KafkaReplay;

public static class Cli
{
    // неверные параметры: сообщение и справка
    public static int Fail(string message)
    {
        Console.WriteLine($"ОТКАЗ: {message}");
        Console.WriteLine();
        Console.WriteLine(CliOptions.Usage);
        return ExitCodes.Blocked;
    }

    // запрет (суммы, цель = источник): только сообщение
    public static int Block(string message)
    {
        Console.WriteLine($"БЛОК: {message}");
        return ExitCodes.Blocked;
    }
}

public static class Connections
{
    public static bool TryResolve(string? bootstrap, string? envName, out ConnectionSettings? settings, out string? error)
    {
        settings = null;
        if (envName is null)
        {
            settings = ConnectionSettings.Plain(bootstrap!);
            error = null;
            return true;
        }
        var connectionString = Environment.GetEnvironmentVariable(envName);
        if (string.IsNullOrWhiteSpace(connectionString)) { error = $"переменная окружения {envName} не задана или пустая"; return false; }
        if (!ConnectionSettings.TryParse(connectionString, out settings, out error)) { error = $"{envName}: {error}"; return false; }
        return true;
    }
}
