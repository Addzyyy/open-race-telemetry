namespace TelemetryIngester.Configuration;

/// <summary>
/// Configuration for the UDP listener. Bound from the <c>Telemetry</c> appsettings section.
/// </summary>
public sealed class TelemetryOptions
{
    /// <summary>
    /// UDP port the F1 game broadcasts telemetry on. Must match the port configured
    /// in-game under Settings → Telemetry. Default: 20777.
    /// </summary>
    public int ListenPort { get; init; } = 20777;

    /// <summary>
    /// When <c>false</c> (default), only the player's own car emits events.
    /// When <c>true</c>, all 20 cars on the grid emit events — useful for
    /// building race engineer tools for the whole field.
    /// </summary>
    public bool AllCars { get; init; }
}
