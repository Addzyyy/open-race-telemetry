namespace TelemetryIngester.Events;

public sealed record LapDataEvent : TelemetryEvent
{
    public required int CurrentLapTimeMs { get; init; }
    public required int CurrentLapNum { get; init; }
    public required int? Sector1TimeMs { get; init; }
    public required int? Sector2TimeMs { get; init; }
    public required int? Sector3TimeMs { get; init; }
    public required float LapDistance { get; init; }
    public required float TotalDistance { get; init; }
    public required int CarPosition { get; init; }
    public required bool CurrentLapInvalid { get; init; }
    public required int Penalties { get; init; }
    public required int NumPitStops { get; init; }
    public required int PitStatus { get; init; }
    public required int Sector { get; init; }
    public required int ResultStatus { get; init; }
}
