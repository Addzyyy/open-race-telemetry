namespace TelemetryIngester.Events;

/// <summary>
/// End-of-race classification result for a single car, emitted once per car when the
/// <c>FinalClassificationData</c> packet arrives at the end of a session.
/// Always emits all classified cars regardless of the AllCars setting — race results
/// are session-wide data, not high-frequency telemetry.
/// Maps to the <c>final-classification</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record FinalClassificationEvent : TelemetryEvent
{
    public required byte Position { get; init; }
    public required byte NumLaps { get; init; }
    public required byte GridPosition { get; init; }
    public required byte Points { get; init; }
    public required byte NumPitStops { get; init; }
    public required byte ResultStatus { get; init; }
    public required byte ResultReason { get; init; }
    public required uint BestLapTimeMs { get; init; }
    public required double TotalRaceTime { get; init; }
    public required byte PenaltiesTime { get; init; }
    public required byte NumPenalties { get; init; }
    public required byte NumTyreStints { get; init; }

    /// <summary>
    /// Comma-separated visual tyre compound IDs for each stint (e.g. "16,17,18").
    /// Derived from <c>TyreStintsVisual</c> on the raw packet.
    /// </summary>
    public required string TyreStintsVisual { get; init; }

    /// <summary>
    /// Comma-separated lap numbers at which each stint ended (e.g. "15,42,58").
    /// Derived from <c>TyreStintsEndLaps</c> on the raw packet.
    /// </summary>
    public required string TyreStintsEndLaps { get; init; }
}
