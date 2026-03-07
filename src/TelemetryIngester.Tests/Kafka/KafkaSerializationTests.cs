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

    [Fact]
    public void Deserialize_CarTelemetryEvent_RoundTrip()
    {
        var original = MakeCarTelemetryEvent();
        var json = KafkaMessageSerializer.Serialize(original);
        var deserialized = KafkaMessageSerializer.Deserialize(json);

        var result = Assert.IsType<CarTelemetryEvent>(deserialized);
        Assert.Equal(original.EventType, result.EventType);
        Assert.Equal(original.SessionUid, result.SessionUid);
        Assert.Equal(original.FrameId, result.FrameId);
        Assert.Equal(original.CarIndex, result.CarIndex);
        Assert.Equal(original.Speed, result.Speed);
        Assert.Equal(original.Throttle, result.Throttle, precision: 5);
        Assert.Equal(original.Steer, result.Steer, precision: 5);
        Assert.Equal(original.Gear, result.Gear);
        Assert.True(result.Drs);
        Assert.Equal(original.BrakesTempRl, result.BrakesTempRl);
        Assert.Equal(original.TyresInnerTempFr, result.TyresInnerTempFr);
    }

    [Fact]
    public void Deserialize_LapDataEvent_RoundTrip()
    {
        var original = new LapDataEvent
        {
            EventType = "LapData",
            SessionUid = "55555",
            Timestamp = new DateTimeOffset(2025, 6, 15, 14, 30, 0, TimeSpan.Zero),
            FrameId = 100u,
            CarIndex = 0,
            CurrentLapTimeMs = 85432,
            CurrentLapNum = 5,
            Sector1TimeMs = 28000,
            Sector2TimeMs = 31000,
            Sector3TimeMs = null,
            LapDistance = 2345.6f,
            TotalDistance = 15000.0f,
            CarPosition = 3,
            CurrentLapInvalid = false,
            Penalties = 0,
            NumPitStops = 1,
            PitStatus = 0,
            Sector = 2,
            ResultStatus = 2,
        };

        var json = KafkaMessageSerializer.Serialize(original);
        var deserialized = KafkaMessageSerializer.Deserialize(json);

        var result = Assert.IsType<LapDataEvent>(deserialized);
        Assert.Equal(original.CurrentLapTimeMs, result.CurrentLapTimeMs);
        Assert.Equal(original.Sector1TimeMs, result.Sector1TimeMs);
        Assert.Null(result.Sector3TimeMs);
        Assert.Equal(original.LapDistance, result.LapDistance, precision: 1);
        Assert.Equal(original.CarPosition, result.CarPosition);
    }

    [Fact]
    public void Deserialize_CarStatusEvent_WithNulls_RoundTrip()
    {
        var original = new CarStatusEvent
        {
            EventType = "CarStatus",
            SessionUid = "111",
            Timestamp = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero),
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

        var json = KafkaMessageSerializer.Serialize(original);
        var deserialized = KafkaMessageSerializer.Deserialize(json);

        var result = Assert.IsType<CarStatusEvent>(deserialized);
        Assert.Equal(0.15f, result.TyreWearRl!.Value, precision: 5);
        Assert.Null(result.TyreWearRr);
        Assert.Null(result.ActualTyreCompound);
        Assert.Null(result.ErsDeployMode);
    }

    [Fact]
    public void Deserialize_UnknownEventType_ThrowsJsonException()
    {
        var json = """{"eventType":"Unknown","sessionUid":"1","timestamp":"2025-01-01T00:00:00+00:00","frameId":1,"carIndex":0,"data":{}}""";
        Assert.Throws<JsonException>(() => KafkaMessageSerializer.Deserialize(json));
    }
}
