namespace TelemetryIngester.Events;

/// <summary>
/// A single weather forecast sample from the game's prediction model.
/// Up to 64 samples per <c>SessionDataPacket</c>, covering future time offsets.
/// Only emitted when the forecast changes (deduplication via hash in PacketMapper).
/// Not per-car — uses <c>CarIndex = 255</c> as a sentinel value.
/// </summary>
public sealed record WeatherForecastEvent : TelemetryEvent
{
    public required int ForecastSessionType { get; init; }
    public required int TimeOffset { get; init; }
    public required int Weather { get; init; }
    public required int TrackTemperature { get; init; }
    public required int TrackTemperatureChange { get; init; }
    public required int AirTemperature { get; init; }
    public required int AirTemperatureChange { get; init; }
    public required int RainPercentage { get; init; }
    public required int ForecastAccuracy { get; init; }
    public required int SampleIndex { get; init; }
}
