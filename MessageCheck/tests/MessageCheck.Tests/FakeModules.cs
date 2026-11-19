using MessageCheck;

namespace MessageCheck.Tests;

// «Сломанные» модули преобразования для тестов

// Тормозит: думает 10 секунд - тл
public class SlowModule : IConverter
{
    public async Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct)
    {
        await Task.Delay(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
        return null;
    }
}

// Тормозит синхронно: работает до того, как вернуть Task (как JsonModule на большом теле)
public class SyncSlowModule : IConverter
{
    public Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct)
    {
        using var never = new ManualResetEventSlim(false);
        never.Wait(TimeSpan.FromSeconds(3));
        return Task.FromResult<object?>(null);
    }
}

// Падает с неожиданной ошибкой
public class CrashingModule : IConverter
{
    public Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct)
        => throw new InvalidOperationException("модуль аварийно завершился");
}

// null или объект не того типа
public class WrongAnswerModule(object? answer) : IConverter
{
    public Task<object?> ConvertAsync(byte[] payload, Type dtoType, CancellationToken ct)
        => Task.FromResult(answer);
}
