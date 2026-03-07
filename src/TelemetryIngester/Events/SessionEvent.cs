namespace TelemetryIngester.Events;

/// <summary>
/// Track, weather, and session metadata, published 2x per second.
/// Not per-car — uses <c>CarIndex = 255</c> as a sentinel value.
/// Maps to the <c>session</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record SessionEvent : TelemetryEvent
{
    public required int Track { get; init; }
    public required int SessionType { get; init; }
    public required int Weather { get; init; }
    public required int TrackTemperature { get; init; }
    public required int AirTemperature { get; init; }
    public required int TotalLaps { get; init; }
    public required int TrackLength { get; init; }
    public required int SessionTimeLeft { get; init; }
    public required int SessionDuration { get; init; }
    public required int SafetyCarStatus { get; init; }
    public required int PitSpeedLimit { get; init; }
    public required int Formula { get; init; }
    public required bool GamePaused { get; init; }
    public required int PitStopWindowIdealLap { get; init; }
    public required int PitStopWindowLatestLap { get; init; }
    public required int PitStopRejoinPosition { get; init; }
}
