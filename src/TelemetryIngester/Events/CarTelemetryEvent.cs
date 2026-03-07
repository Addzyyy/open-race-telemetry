namespace TelemetryIngester.Events;

/// <summary>
/// Real-time driving inputs and sensor readings for a single car, sampled at ~60 Hz.
/// Throttle, brake, and steer are normalised floats (0.0–1.0 or -1.0–+1.0).
/// Temperatures are in degrees Celsius. Speed is in km/h.
/// Tyre position suffixes: Rl = Rear Left, Rr = Rear Right, Fl = Front Left, Fr = Front Right.
/// Maps to the <c>car-telemetry</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record CarTelemetryEvent : TelemetryEvent
{
    public required int Speed { get; init; }
    public required float Throttle { get; init; }
    public required float Steer { get; init; }
    public required float Brake { get; init; }
    public required int Clutch { get; init; }

    /// <summary>Current gear. Negative = reverse, 0 = neutral, 1–8 = forward gears.</summary>
    public required int Gear { get; init; }

    public required int EngineRpm { get; init; }

    /// <summary>
    /// Whether DRS (Drag Reduction System) is active. DRS opens a flap in the rear wing
    /// to reduce drag on straights when permitted by race rules.
    /// </summary>
    public required bool Drs { get; init; }

    /// <summary>How far along the RPM range the rev-lights are (0–100 %). 100 % = optimal shift point.</summary>
    public required int RevLightsPercent { get; init; }

    public required int BrakesTempRl { get; init; }
    public required int BrakesTempRr { get; init; }
    public required int BrakesTempFl { get; init; }
    public required int BrakesTempFr { get; init; }
    public required int TyresSurfaceTempRl { get; init; }
    public required int TyresSurfaceTempRr { get; init; }
    public required int TyresSurfaceTempFl { get; init; }
    public required int TyresSurfaceTempFr { get; init; }

    /// <summary>
    /// Inner (carcass) temperature — measured inside the tyre body rather than at the surface.
    /// A better indicator of whether the tyre is in its optimal operating window.
    /// </summary>
    public required int TyresInnerTempRl { get; init; }
    public required int TyresInnerTempRr { get; init; }
    public required int TyresInnerTempFl { get; init; }
    public required int TyresInnerTempFr { get; init; }
}
