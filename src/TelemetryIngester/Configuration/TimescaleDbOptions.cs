namespace TelemetryIngester.Configuration;

/// <summary>
/// Connection settings for TimescaleDB. Bound from the <c>TimescaleDb</c> appsettings section.
/// Used by the storage layer (next PR) — defined here so the full config shape is in place.
/// </summary>
public sealed class TimescaleDbOptions
{
    /// <summary>Npgsql connection string for the TimescaleDB instance.</summary>
    public string ConnectionString { get; init; } = string.Empty;
}
