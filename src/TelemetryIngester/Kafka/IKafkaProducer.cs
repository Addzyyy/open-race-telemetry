using TelemetryIngester.Events;

namespace TelemetryIngester.Kafka;

/// <summary>
/// Publishes telemetry events to the appropriate Kafka topic.
/// Kafka acts as the message bus between the UDP ingestion layer and the
/// TimescaleDB storage layer, decoupling them so either side can be restarted
/// independently without losing data.
/// </summary>
public interface IKafkaProducer : IAsyncDisposable
{
    /// <summary>
    /// Serialises <paramref name="event"/> and publishes it to the Kafka topic
    /// that corresponds to its <see cref="TelemetryEvent.EventType"/>.
    /// <para>
    /// This method uses fire-and-forget semantics: failures are logged as warnings
    /// rather than thrown, because a dropped telemetry frame is acceptable whereas
    /// crashing the ingestion loop is not.
    /// </para>
    /// </summary>
    /// <param name="event">The canonical event to publish.</param>
    /// <param name="cancellationToken">Propagated from the host shutdown token.</param>
    Task ProduceAsync(TelemetryEvent @event, CancellationToken cancellationToken = default);
}
