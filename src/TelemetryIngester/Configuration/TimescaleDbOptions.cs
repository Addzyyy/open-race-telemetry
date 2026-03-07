namespace TelemetryIngester.Configuration;

public sealed class TimescaleDbOptions
{
    public string ConnectionString { get; init; } = string.Empty;
}
