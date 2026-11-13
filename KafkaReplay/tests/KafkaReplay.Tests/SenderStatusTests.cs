using Confluent.Kafka;
using KafkaReplay;

namespace KafkaReplay.Tests;

// какой статус ставить, если отправка закончилась ошибкой уже после постановки в очередь
public class SenderStatusTests
{
    [Fact]
    public void Local_timeout_is_unknown_not_not_written()
    {
        // так было на dev: «Local: Message timed out» с NotPersisted, а сообщение в топике есть
        Assert.Equal(DeliveryStatus.Unknown, KafkaPlanSender.StatusOnError(isLocalError: true, PersistenceStatus.NotPersisted));
    }

    [Fact]
    public void Broker_said_not_persisted_is_not_written()
    {
        // брокер ответил отказом (например, сообщение слишком большое) — точно не записано
        Assert.Equal(DeliveryStatus.NotWritten, KafkaPlanSender.StatusOnError(isLocalError: false, PersistenceStatus.NotPersisted));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Possibly_persisted_is_unknown(bool isLocalError) =>
        Assert.Equal(DeliveryStatus.Unknown, KafkaPlanSender.StatusOnError(isLocalError, PersistenceStatus.PossiblyPersisted));
}
