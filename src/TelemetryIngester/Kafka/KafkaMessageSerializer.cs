using System.Text;
using System.Text.Json;
using TelemetryIngester.Events;

namespace TelemetryIngester.Kafka;

/// <summary>
/// Serialises a <see cref="TelemetryEvent"/> into the canonical Kafka message envelope:
/// <code>
/// {
///   "eventType": "CarTelemetry",
///   "sessionUid": "...",
///   "timestamp": "...",
///   "frameId": 12345,
///   "carIndex": 0,
///   "data": { /* domain-specific fields */ }
/// }
/// </code>
/// The five base fields are promoted to the top level so Kafka consumers can filter by
/// event type or session without deserialising the nested <c>data</c> object.
/// </summary>
internal static class KafkaMessageSerializer
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // The five fields that live at the top level of the envelope rather than inside "data".
    private static readonly HashSet<string> BasePropertyNames =
        ["eventType", "sessionUid", "timestamp", "frameId", "carIndex"];

    /// <summary>
    /// Deserialises a JSON envelope back into the correct <see cref="TelemetryEvent"/> subtype.
    /// Reads the <c>eventType</c> field to determine the concrete type, then merges the
    /// top-level base fields with the nested <c>data</c> object into a flat JSON object
    /// for deserialization.
    /// </summary>
    internal static TelemetryEvent Deserialize(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var eventType = root.GetProperty("eventType").GetString();

        // Determine the target concrete type from the eventType discriminator.
        var targetType = eventType switch
        {
            "CarTelemetry" => typeof(CarTelemetryEvent),
            "LapData" => typeof(LapDataEvent),
            "CarStatus" => typeof(CarStatusEvent),
            "Participant" => typeof(ParticipantEvent),
            "Session" => typeof(SessionEvent),
            "SessionHistory" => typeof(SessionHistoryEvent),
            _ => throw new JsonException($"Unknown event type: {eventType}"),
        };

        // Merge base fields + data fields into a single flat JSON object.
        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms);

        writer.WriteStartObject();

        // Write all top-level properties except "data" (they are the base fields).
        foreach (var prop in root.EnumerateObject())
        {
            if (prop.Name != "data")
                prop.WriteTo(writer);
        }

        // Flatten the nested "data" object into the top level.
        if (root.TryGetProperty("data", out var data))
        {
            foreach (var prop in data.EnumerateObject())
                prop.WriteTo(writer);
        }

        writer.WriteEndObject();
        writer.Flush();

        var flatJson = Encoding.UTF8.GetString(ms.ToArray());
        return (TelemetryEvent)JsonSerializer.Deserialize(flatJson, targetType, JsonOptions)!;
    }

    /// <summary>
    /// Serialises <paramref name="event"/> to the envelope JSON format described above.
    /// </summary>
    internal static string Serialize(TelemetryEvent @event)
    {
        // First pass: serialise the full event record to JSON so we can iterate its properties.
        // We use the concrete runtime type (not TelemetryEvent) so derived-class properties are included.
        var json = JsonSerializer.Serialize(@event, @event.GetType(), JsonOptions);
        using var doc = JsonDocument.Parse(json);

        // Second pass: rebuild the JSON in the envelope shape using a streaming writer.
        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms);

        writer.WriteStartObject();

        // Write the five base fields at the top level with their exact names.
        writer.WriteString("eventType", @event.EventType);
        writer.WriteString("sessionUid", @event.SessionUid);
        writer.WriteString("timestamp", @event.Timestamp.ToString("O")); // ISO 8601 round-trip format
        writer.WriteNumber("frameId", @event.FrameId);
        writer.WriteNumber("carIndex", @event.CarIndex);

        // Write all remaining (domain-specific) properties nested under "data".
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
