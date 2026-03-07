using TelemetryIngester.Events;

namespace TelemetryIngester.Kafka;

public interface IKafkaProducer : IAsyncDisposable
{
    Task ProduceAsync(TelemetryEvent @event, CancellationToken cancellationToken = default);
}
