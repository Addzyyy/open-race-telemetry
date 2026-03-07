using System.Text.Json;
using TelemetryIngester.Events;
using TelemetryIngester.Kafka;

namespace TelemetryIngester.Tests.Kafka;

public sealed class KafkaSerializationTests
{
    private static CarTelemetryEvent MakeCarTelemetryEvent() => new()
    {
        EventType = "CarTelemetry",
        SessionUid = "99999",
        Timestamp = new DateTimeOffset(2025, 1, 1, 12, 0, 0, TimeSpan.Zero),
        FrameId = 42u,
        CarIndex = 3,
        Speed = 300,
        Throttle = 1.0f,
        Steer = -0.5f,
        Brake = 0.0f,
        Clutch = 0,
        Gear = 7,
        EngineRpm = 11500,
        Drs = true,
        RevLightsPercent = 85,
        BrakesTempRl = 500,
        BrakesTempRr = 510,
        BrakesTempFl = 490,
        BrakesTempFr = 505,
        TyresSurfaceTempRl = 90,
        TyresSurfaceTempRr = 91,
        TyresSurfaceTempFl = 88,
        TyresSurfaceTempFr = 89,
        TyresInnerTempRl = 100,
        TyresInnerTempRr = 101,
        TyresInnerTempFl = 98,
        TyresInnerTempFr = 99,
    };

    [Fact]
    public void Serialize_EnvelopeHasTopLevelBaseFields()
    {
        var @event = MakeCarTelemetryEvent();
        var json = KafkaMessageSerializer.Serialize(@event);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("CarTelemetry", root.GetProperty("eventType").GetString());
        Assert.Equal("99999", root.GetProperty("sessionUid").GetString());
        Assert.Equal(42u, root.GetProperty("frameId").GetUInt32());
        Assert.Equal(3, root.GetProperty("carIndex").GetByte());
    }

    [Fact]
    public void Serialize_DomainFieldsNestedUnderData()
    {
        var @event = MakeCarTelemetryEvent();
        var json = KafkaMessageSerializer.Serialize(@event);
        using var doc = JsonDocument.Parse(json);
        var data = doc.RootElement.GetProperty("data");

        Assert.Equal(300, data.GetProperty("speed").GetInt32());
        Assert.Equal(1.0f, data.GetProperty("throttle").GetSingle(), precision: 5);
        Assert.Equal(7, data.GetProperty("gear").GetInt32());
        Assert.True(data.GetProperty("drs").GetBoolean());
    }

    [Fact]
    public void Serialize_PropertyNamesAreCamelCase()
    {
        var @event = MakeCarTelemetryEvent();
        var json = KafkaMessageSerializer.Serialize(@event);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("eventType", out _));
        Assert.True(root.TryGetProperty("sessionUid", out _));
        Assert.True(root.TryGetProperty("frameId", out _));
        Assert.True(root.TryGetProperty("carIndex", out _));

        var data = root.GetProperty("data");
        Assert.True(data.TryGetProperty("brakesTempRl", out _));
        Assert.True(data.TryGetProperty("tyresSurfaceTempRl", out _));
    }

    [Fact]
    public void Serialize_NullableFields_SerializeCorrectly()
    {
        var @event = new CarStatusEvent
        {
            EventType = "CarStatus",
            SessionUid = "111",
            Timestamp = DateTimeOffset.UtcNow,
            FrameId = 1u,
            CarIndex = 0,
            TyreWearRl = 0.15f,
            TyreWearRr = null,
            TyreWearFl = null,
            TyreWearFr = null,
            ActualTyreCompound = null,
            VisualTyreCompound = null,
            TyresAgeLaps = null,
            FuelInTank = null,
            FuelCapacity = null,
            FuelRemainingLaps = null,
            ErsStoreEnergy = null,
            ErsDeployMode = null,
            ErsHarvestedLapMguk = null,
            ErsHarvestedLapMguh = null,
            ErsDeployedLap = null,
        };

        var json = KafkaMessageSerializer.Serialize(@event);
        using var doc = JsonDocument.Parse(json);
        var data = doc.RootElement.GetProperty("data");

        Assert.Equal(0.15f, data.GetProperty("tyreWearRl").GetSingle(), precision: 5);
        Assert.Equal(JsonValueKind.Null, data.GetProperty("tyreWearRr").ValueKind);
    }

    [Fact]
    public void Serialize_BaseFieldsNotDuplicatedInData()
    {
        var @event = MakeCarTelemetryEvent();
        var json = KafkaMessageSerializer.Serialize(@event);
        using var doc = JsonDocument.Parse(json);
        var data = doc.RootElement.GetProperty("data");

        Assert.False(data.TryGetProperty("eventType", out _));
        Assert.False(data.TryGetProperty("sessionUid", out _));
        Assert.False(data.TryGetProperty("frameId", out _));
        Assert.False(data.TryGetProperty("carIndex", out _));
        Assert.False(data.TryGetProperty("timestamp", out _));
    }
}
