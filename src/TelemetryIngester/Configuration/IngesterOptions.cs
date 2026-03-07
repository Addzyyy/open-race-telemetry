namespace TelemetryIngester.Configuration;

/// <summary>
/// Tuning parameters for the Kafka consumer and TimescaleDB batch writer.
/// Bound from the <c>Ingester</c> appsettings section.
/// Used by the storage layer (next PR) — defined here so the full config shape is in place.
/// </summary>
public sealed class IngesterOptions
{
    /// <summary>
    /// Number of events to accumulate before flushing a batch to TimescaleDB.
    /// Larger batches are more efficient but increase write latency.
    /// </summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>
    /// Maximum time in milliseconds to wait before flushing a partial batch.
    /// Prevents data from getting stuck when event volume is low.
    /// </summary>
    public int FlushIntervalMs { get; init; } = 500;
}
