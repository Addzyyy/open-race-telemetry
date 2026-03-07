namespace TelemetryIngester.Events;

/// <summary>
/// Driver/team metadata for a single car, published every 5 seconds.
/// Always emits all active cars regardless of the AllCars setting — participant names are
/// session metadata, not high-frequency telemetry.
/// Maps to the <c>participants</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record ParticipantEvent : TelemetryEvent
{
    public required string Name { get; init; }
    public required int Team { get; init; }
    public required int RaceNumber { get; init; }
    public required int Nationality { get; init; }
    public required bool IsAiControlled { get; init; }
    public required int Driver { get; init; }
    public required int Platform { get; init; }
    public required bool IsMyTeam { get; init; }
    public required bool IsTelemetryPublic { get; init; }
}
