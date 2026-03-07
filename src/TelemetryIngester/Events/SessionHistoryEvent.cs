namespace TelemetryIngester.Events;

/// <summary>
/// Per-car lap history summary, published ~1/second per car (cycling through 20 cars).
/// Contains best lap/sector metadata plus the latest completed lap's sector breakdown,
/// including sector 3 times which are not available from the regular LapData packet.
/// Maps to the <c>session-history</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record SessionHistoryEvent : TelemetryEvent
{
    public required int NumLaps { get; init; }
    public required int NumTyreStints { get; init; }
    public required int BestLapTimeLapNum { get; init; }
    public required int BestSector1LapNum { get; init; }
    public required int BestSector2LapNum { get; init; }
    public required int BestSector3LapNum { get; init; }
    public required int? LatestLapTimeMs { get; init; }
    public required int? LatestSector1TimeMs { get; init; }
    public required int? LatestSector2TimeMs { get; init; }
    public required int? LatestSector3TimeMs { get; init; }
    public required bool? LatestLapValid { get; init; }
}
