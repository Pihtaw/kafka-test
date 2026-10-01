using MessageCheck;

namespace MessageCheck.Tests;

// «Сломанные» модули преобразования для тестов

// Тормозит: думает 10 секунд - тл
public class SlowModule : IConverter
{
    public async Task<object?> ConvertAsync(string payload, Type dtoType, CancellationToken ct)
    {
        await Task.Delay(TimeSpan.FromSeconds(10), ct);
        return null;
    }
}

// Падает с неожиданной ошибкой
public class CrashingModule : IConverter
{
    public Task<object?> ConvertAsync(string payload, Type dtoType, CancellationToken ct)
        => throw new InvalidOperationException("модуль аварийно завершился");
}

// null или объект не того типа
public class WrongAnswerModule(object? answer) : IConverter
{
    public Task<object?> ConvertAsync(string payload, Type dtoType, CancellationToken ct)
        => Task.FromResult(answer);
}
