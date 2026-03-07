using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;

namespace TelemetryIngester.Kafka;

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

    public async Task ProduceAsync(TelemetryEvent @event, CancellationToken cancellationToken = default)
    {
        var topic = @event.EventType switch
        {
            "CarTelemetry" => "car-telemetry",
            "LapData" => "lap-data",
            "CarStatus" => "car-status",
            _ => null,
        };

        if (topic is null)
        {
            _logger.LogWarning("No topic mapping for event type {EventType}", @event.EventType);
            return;
        }

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

    public async ValueTask DisposeAsync()
    {
        await Task.Run(() => _producer.Flush(TimeSpan.FromSeconds(5)));
        _producer.Dispose();
    }
}
