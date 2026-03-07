namespace TelemetryIngester.Configuration;

/// <summary>
/// Connection settings for the Kafka broker. Bound from the <c>Kafka</c> appsettings section.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>
    /// Comma-separated list of Kafka broker addresses (host:port).
    /// For the local Docker Compose setup this is <c>localhost:9092</c>.
    /// </summary>
    public string BootstrapServers { get; init; } = "localhost:9092";

    /// <summary>
    /// Consumer group ID used when reading from Kafka (relevant for the consumer side, next PR).
    /// All consumers in the same group share the workload across partitions.
    /// </summary>
    public string GroupId { get; init; } = "telemetry-ingester";
}
