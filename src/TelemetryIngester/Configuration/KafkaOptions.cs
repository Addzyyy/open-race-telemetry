namespace TelemetryIngester.Configuration;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string GroupId { get; init; } = "telemetry-ingester";
}
