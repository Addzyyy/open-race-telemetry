namespace TelemetryIngester.Configuration;

public sealed class IngesterOptions
{
    public int BatchSize { get; init; } = 100;
    public int FlushIntervalMs { get; init; } = 500;
}
