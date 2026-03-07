namespace TelemetryIngester.Events;

public sealed record CarTelemetryEvent : TelemetryEvent
{
    public required int Speed { get; init; }
    public required float Throttle { get; init; }
    public required float Steer { get; init; }
    public required float Brake { get; init; }
    public required int Clutch { get; init; }
    public required int Gear { get; init; }
    public required int EngineRpm { get; init; }
    public required bool Drs { get; init; }
    public required int RevLightsPercent { get; init; }
    public required int BrakesTempRl { get; init; }
    public required int BrakesTempRr { get; init; }
    public required int BrakesTempFl { get; init; }
    public required int BrakesTempFr { get; init; }
    public required int TyresSurfaceTempRl { get; init; }
    public required int TyresSurfaceTempRr { get; init; }
    public required int TyresSurfaceTempFl { get; init; }
    public required int TyresSurfaceTempFr { get; init; }
    public required int TyresInnerTempRl { get; init; }
    public required int TyresInnerTempRr { get; init; }
    public required int TyresInnerTempFl { get; init; }
    public required int TyresInnerTempFr { get; init; }
}
