namespace TelemetryIngester.Events;

public abstract record TelemetryEvent
{
    public required string EventType { get; init; }
    public required string SessionUid { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required uint FrameId { get; init; }
    public required byte CarIndex { get; init; }
}
