namespace TelemetryIngester.Events;

/// <summary>
/// Positional and timing data for a single car on its current lap, sampled at ~60 Hz.
/// Sector times are nullable — they are only populated once the car crosses the sector boundary.
/// Maps to the <c>lap-data</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record LapDataEvent : TelemetryEvent
{
    public required int CurrentLapTimeMs { get; init; }
    public required int CurrentLapNum { get; init; }
    public required int? Sector1TimeMs { get; init; }
    public required int? Sector2TimeMs { get; init; }

    /// <summary>
    /// Always <c>null</c> from this packet source. The game does not include sector 3 in the
    /// per-frame LapData stream; it becomes available in LapHistoryDataPacket after the lap completes.
    /// </summary>
    public required int? Sector3TimeMs { get; init; }

    public required float LapDistance { get; init; }
    public required float TotalDistance { get; init; }
    public required int CarPosition { get; init; }
    public required bool CurrentLapInvalid { get; init; }
    public required int Penalties { get; init; }
    public required int NumPitStops { get; init; }

    /// <summary>Pit status code from F1 UDP spec: 0 = none, 1 = pitting, 2 = in pit area.</summary>
    public required int PitStatus { get; init; }

    /// <summary>Current sector: 0 = sector 1, 1 = sector 2, 2 = sector 3.</summary>
    public required int Sector { get; init; }

    /// <summary>
    /// Result status code from F1 UDP spec: 0 = invalid, 1 = inactive, 2 = active,
    /// 3 = finished, 4 = DNF, 5 = DSQ, etc.
    /// </summary>
    public required int ResultStatus { get; init; }
}
