using F1Game.UDP.Data;
using F1Game.UDP.Packets;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Mapping;

namespace TelemetryIngester.Tests.Mapping;

public sealed class PacketMapperTests
{
    private static PacketMapper CreateMapper(bool allCars = false) =>
        new(Options.Create(new TelemetryOptions { ListenPort = 20777, AllCars = allCars }));

    private static PacketHeader MakeHeader(ulong sessionUid = 999, uint frameId = 42, byte playerCarIndex = 0) =>
        new() { SessionUID = sessionUid, FrameIdentifier = frameId, PlayerCarIndex = playerCarIndex };

    [Fact]
    public void MapCarTelemetryData_MapsSpeedAndThrottle()
    {
        var mapper = CreateMapper();
        var data = new CarTelemetryData
        {
            Speed = 300,
            Throttle = 1.0f,
            Steer = -0.5f,
            Brake = 0.0f,
            Gear = 7,
            EngineRPM = 11500,
            IsDrsOn = true,
            RevLightsPercent = 85,
        };
        var header = MakeHeader(sessionUid: 12345, frameId: 100, playerCarIndex: 3);

        var @event = mapper.MapCarTelemetryData(data, header, 3, DateTimeOffset.UtcNow);

        Assert.Equal("CarTelemetry", @event.EventType);
        Assert.Equal("12345", @event.SessionUid);
        Assert.Equal(100u, @event.FrameId);
        Assert.Equal(3, @event.CarIndex);
        Assert.Equal(300, @event.Speed);
        Assert.Equal(1.0f, @event.Throttle);
        Assert.Equal(-0.5f, @event.Steer);
        Assert.Equal(7, @event.Gear);
        Assert.Equal(11500, @event.EngineRpm);
        Assert.True(@event.Drs);
        Assert.Equal(85, @event.RevLightsPercent);
    }

    [Fact]
    public void MapCarTelemetryData_TyreTempsOrderedRlRrFlFr()
    {
        var mapper = CreateMapper();
        var data = new CarTelemetryData
        {
            BrakesTemperature = new Tyres<ushort>
            {
                RearLeft = 500, RearRight = 510, FrontLeft = 490, FrontRight = 505,
            },
            TyresSurfaceTemperature = new Tyres<byte>
            {
                RearLeft = 90, RearRight = 91, FrontLeft = 88, FrontRight = 89,
            },
            TyresInnerTemperature = new Tyres<byte>
            {
                RearLeft = 100, RearRight = 101, FrontLeft = 98, FrontRight = 99,
            },
        };

        var @event = mapper.MapCarTelemetryData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);

        Assert.Equal(500, @event.BrakesTempRl);
        Assert.Equal(510, @event.BrakesTempRr);
        Assert.Equal(490, @event.BrakesTempFl);
        Assert.Equal(505, @event.BrakesTempFr);
        Assert.Equal(90, @event.TyresSurfaceTempRl);
        Assert.Equal(91, @event.TyresSurfaceTempRr);
        Assert.Equal(88, @event.TyresSurfaceTempFl);
        Assert.Equal(89, @event.TyresSurfaceTempFr);
        Assert.Equal(100, @event.TyresInnerTempRl);
        Assert.Equal(101, @event.TyresInnerTempRr);
        Assert.Equal(98, @event.TyresInnerTempFl);
        Assert.Equal(99, @event.TyresInnerTempFr);
    }

    [Fact]
    public void MapCarTelemetryData_DrsTrue_WhenIsDrsOnTrue()
    {
        var mapper = CreateMapper();
        var data = new CarTelemetryData { IsDrsOn = true };
        var @event = mapper.MapCarTelemetryData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);
        Assert.True(@event.Drs);
    }

    [Fact]
    public void MapCarTelemetryData_DrsFalse_WhenIsDrsOnFalse()
    {
        var mapper = CreateMapper();
        var data = new CarTelemetryData { IsDrsOn = false };
        var @event = mapper.MapCarTelemetryData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);
        Assert.False(@event.Drs);
    }

    [Fact]
    public void MapPacket_PlayerCarOnly_WhenAllCarsFalse()
    {
        var mapper = CreateMapper(allCars: false);
        var header = MakeHeader(playerCarIndex: 5);
        var packetData = new CarTelemetryDataPacket { Header = header };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Single(events);
        Assert.Equal(5, events[0].CarIndex);
    }

    [Fact]
    public void MapPacket_AllCars_WhenAllCarsTrue()
    {
        var mapper = CreateMapper(allCars: true);
        var header = MakeHeader(playerCarIndex: 0);
        var packetData = new CarTelemetryDataPacket { Header = header };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Equal(20, events.Count);
    }

    [Fact]
    public void MapLapData_SectorTimeCombined()
    {
        var mapper = CreateMapper();
        var data = new LapData
        {
            Sector1TimeInMS = 500,
            Sector1TimeInMinutes = 1,
            Sector2TimeInMS = 300,
            Sector2TimeInMinutes = 0,
        };

        var @event = mapper.MapLapData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);

        Assert.Equal(1 * 60_000 + 500, @event.Sector1TimeMs);
        Assert.Equal(300, @event.Sector2TimeMs);
    }

    [Fact]
    public void MapLapData_SectorTimeNull_WhenBothPartsZero()
    {
        var mapper = CreateMapper();
        var data = new LapData
        {
            Sector1TimeInMS = 0,
            Sector1TimeInMinutes = 0,
            Sector2TimeInMS = 0,
            Sector2TimeInMinutes = 0,
        };

        var @event = mapper.MapLapData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);

        Assert.Null(@event.Sector1TimeMs);
        Assert.Null(@event.Sector2TimeMs);
        Assert.Null(@event.Sector3TimeMs);
    }

    [Fact]
    public void MapCarDamageData_HasTyreWear_NullStatusFields()
    {
        var mapper = CreateMapper();
        var data = new CarDamageData
        {
            TyresWear = new Tyres<float>
            {
                RearLeft = 0.1f, RearRight = 0.2f, FrontLeft = 0.3f, FrontRight = 0.4f,
            },
        };

        var @event = mapper.MapCarDamageData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);

        Assert.Equal("CarStatus", @event.EventType);
        Assert.Equal(0.1f, @event.TyreWearRl);
        Assert.Equal(0.2f, @event.TyreWearRr);
        Assert.Equal(0.3f, @event.TyreWearFl);
        Assert.Equal(0.4f, @event.TyreWearFr);
        Assert.Null(@event.ActualTyreCompound);
        Assert.Null(@event.FuelInTank);
        Assert.Null(@event.ErsStoreEnergy);
    }

    [Fact]
    public void MapCarStatusData_HasStatusFields_NullTyreWear()
    {
        var mapper = CreateMapper();
        var data = new CarStatusData
        {
            FuelInTank = 25.5f,
            FuelCapacity = 100.0f,
            TyresAgeLaps = 10,
        };

        var @event = mapper.MapCarStatusData(data, MakeHeader(), 0, DateTimeOffset.UtcNow);

        Assert.Equal("CarStatus", @event.EventType);
        Assert.Null(@event.TyreWearRl);
        Assert.Null(@event.TyreWearRr);
        Assert.Null(@event.TyreWearFl);
        Assert.Null(@event.TyreWearFr);
        Assert.Equal(25.5f, @event.FuelInTank);
        Assert.Equal(100.0f, @event.FuelCapacity);
        Assert.Equal(10, @event.TyresAgeLaps);
    }
}
