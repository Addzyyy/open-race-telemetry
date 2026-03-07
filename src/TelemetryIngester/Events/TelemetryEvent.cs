namespace TelemetryIngester.Events;

/// <summary>
/// Base record for every telemetry event published to Kafka.
/// All events share these five fields so consumers can route and correlate
/// messages without needing to know the specific event type.
/// </summary>
public abstract record TelemetryEvent
{
    /// <summary>
    /// Discriminator that identifies the event type (e.g. "CarTelemetry", "LapData", "CarStatus").
    /// Used by the Kafka producer to pick the right topic.
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>
    /// Unique identifier for the current game session (race, qualifying, practice, etc.).
    /// Stored as a string because the raw value is a 64-bit integer which exceeds JSON's safe integer range.
    /// </summary>
    public required string SessionUid { get; init; }

    /// <summary>Wall-clock time (UTC) at which this event was mapped from the raw UDP packet.</summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>Sequential frame counter incremented by the game at ~60 Hz. Useful for detecting dropped packets.</summary>
    public required uint FrameId { get; init; }

    /// <summary>Zero-based index of the car this event describes (0–19).</summary>
    public required byte CarIndex { get; init; }
}
