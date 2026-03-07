using System.Text;
using System.Text.Json;
using TelemetryIngester.Events;

namespace TelemetryIngester.Kafka;

internal static class KafkaMessageSerializer
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static readonly HashSet<string> BasePropertyNames =
        ["eventType", "sessionUid", "timestamp", "frameId", "carIndex"];

    internal static string Serialize(TelemetryEvent @event)
    {
        var json = JsonSerializer.Serialize(@event, @event.GetType(), JsonOptions);
        using var doc = JsonDocument.Parse(json);

        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms);

        writer.WriteStartObject();
        writer.WriteString("eventType", @event.EventType);
        writer.WriteString("sessionUid", @event.SessionUid);
        writer.WriteString("timestamp", @event.Timestamp.ToString("O"));
        writer.WriteNumber("frameId", @event.FrameId);
        writer.WriteNumber("carIndex", @event.CarIndex);

        writer.WritePropertyName("data");
        writer.WriteStartObject();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            if (!BasePropertyNames.Contains(prop.Name))
                prop.WriteTo(writer);
        }
        writer.WriteEndObject();

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(ms.ToArray());
    }
}
