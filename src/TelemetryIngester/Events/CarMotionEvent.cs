namespace TelemetryIngester.Events;

/// <summary>
/// G-force and motion data for a single car, sampled at ~60 Hz.
/// G-forces are in units of g (1 g ≈ 9.81 m/s²).
/// Maps to the <c>car-motion</c> Kafka topic and TimescaleDB hypertable.
/// </summary>
public sealed record CarMotionEvent : TelemetryEvent
{
    public required float GForceLateral { get; init; }
    public required float GForceLongitudinal { get; init; }
    public required float GForceVertical { get; init; }
}
