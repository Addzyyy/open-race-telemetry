using TelemetryIngester.Events;

namespace TelemetryIngester.Storage;

public interface ITimescaleWriter
{
    Task WriteBatchAsync(IReadOnlyList<TelemetryEvent> events, CancellationToken ct);
}
