namespace TelemetryIngester.Configuration;

public sealed class TelemetryOptions
{
    public int ListenPort { get; init; } = 20777;
    public bool AllCars { get; init; }
}
