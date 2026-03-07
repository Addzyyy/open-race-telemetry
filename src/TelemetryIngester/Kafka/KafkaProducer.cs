using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;

namespace TelemetryIngester.Kafka;

/// <summary>
/// Publishes telemetry events to Kafka topics using the Confluent Kafka producer.
/// Failures are logged as warnings rather than thrown — dropping an occasional telemetry
/// frame is preferable to crashing the ingestion loop.
/// </summary>
public sealed class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;

    public KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        var config = new ProducerConfig { BootstrapServers = options.Value.BootstrapServers };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    /// <summary>
    /// Routes the event to the correct Kafka topic, serialises it, and publishes it.
    /// </summary>
    public async Task ProduceAsync(TelemetryEvent @event, CancellationToken cancellationToken = default)
    {
        // Map event type to Kafka topic name.
        var topic = @event.EventType switch
        {
            "CarTelemetry"   => "car-telemetry",
            "LapData"        => "lap-data",
            "CarStatus"      => "car-status",
            "Participant"    => "participants",
            "Session"        => "session",
            "SessionHistory" => "session-history",
            _                => null,
        };

        if (topic is null)
        {
            _logger.LogWarning("No topic mapping for event type {EventType}", @event.EventType);
            return;
        }

        // Use session+car as the partition key so all messages for the same car in the same
        // session always land on the same Kafka partition, preserving ordering.
        var key = $"{@event.SessionUid}-{@event.CarIndex}";
        var value = KafkaMessageSerializer.Serialize(@event);

        try
        {
            await _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = value }, cancellationToken);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogWarning(ex, "Failed to produce message to topic {Topic}", topic);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Unexpected error producing to topic {Topic}", topic);
        }
    }

    /// <summary>
    /// Flushes any buffered messages to Kafka and disposes the producer on shutdown.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Flush blocks until all buffered messages are delivered or the timeout expires.
        // Wrapped in Task.Run because the Confluent SDK's Flush is synchronous.
        await Task.Run(() => _producer.Flush(TimeSpan.FromSeconds(5)));
        _producer.Dispose();
    }
}
