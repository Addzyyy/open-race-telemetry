using F1Game.UDP.Data;
using F1Game.UDP.Enums;
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

    // ── Participant tests ──────────────────────────────────────────────────────

    [Fact]
    public void MapParticipantData_MapsAllFields()
    {
        var mapper = CreateMapper();
        var data = new ParticipantData
        {
            Name = "Verstappen",
            Team = Team.RedBullRacing,
            RaceNumber = 1,
            Nationality = Nationality.Dutch,
            IsAiControlled = false,
            Driver = Driver.MaxVerstappen,
            Platform = Platform.Steam,
            IsMyTeam = false,
            IsTelemetryPublic = true,
        };
        var header = MakeHeader(sessionUid: 5000, frameId: 77, playerCarIndex: 0);

        var @event = mapper.MapParticipantData(data, header, 0, DateTimeOffset.UtcNow);

        Assert.Equal("Participant", @event.EventType);
        Assert.Equal("5000", @event.SessionUid);
        Assert.Equal(77u, @event.FrameId);
        Assert.Equal(0, @event.CarIndex);
        Assert.Equal("Verstappen", @event.Name);
        Assert.Equal((int)Team.RedBullRacing, @event.Team);
        Assert.Equal(1, @event.RaceNumber);
        Assert.Equal((int)Nationality.Dutch, @event.Nationality);
        Assert.False(@event.IsAiControlled);
        Assert.Equal((int)Driver.MaxVerstappen, @event.Driver);
        Assert.Equal((int)Platform.Steam, @event.Platform);
        Assert.False(@event.IsMyTeam);
        Assert.True(@event.IsTelemetryPublic);
    }

    [Fact]
    public void MapPacket_Participants_AlwaysEmitsAllActiveCars()
    {
        var mapper = CreateMapper(allCars: false);
        var header = MakeHeader(playerCarIndex: 3);
        var packetData = new ParticipantsDataPacket { Header = header, NumActiveCars = 5 };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Equal(5, events.Count);
        // All 5 active cars should be emitted, not just the player car.
        for (byte i = 0; i < 5; i++)
            Assert.Equal(i, events[i].CarIndex);
    }

    // ── Session tests ──────────────────────────────────────────────────────────

    [Fact]
    public void MapSessionData_MapsAllFields()
    {
        var mapper = CreateMapper();
        var header = MakeHeader(sessionUid: 8888, frameId: 200);
        var data = new SessionDataPacket
        {
            Header = header,
            Track = Track.Silverstone,
            SessionType = SessionType.Race,
            Weather = Weather.LightRain,
            TrackTemperature = 35,
            AirTemperature = 22,
            TotalLaps = 52,
            TrackLength = 5891,
            SessionTimeLeft = 3600,
            SessionDuration = 7200,
            SafetyCarStatus = SafetyCarType.NoSafetyCar,
            PitSpeedLimit = 80,
            Formula = FormulaType.F1Modern,
            GamePaused = false,
            PitStopWindowIdealLap = 18,
            PitStopWindowLatestLap = 25,
            PitStopRejoinPosition = 5,
        };

        var @event = mapper.MapSessionData(data, header, DateTimeOffset.UtcNow);

        Assert.Equal("Session", @event.EventType);
        Assert.Equal("8888", @event.SessionUid);
        Assert.Equal(200u, @event.FrameId);
        Assert.Equal(255, @event.CarIndex);
        Assert.Equal((int)Track.Silverstone, @event.Track);
        Assert.Equal((int)SessionType.Race, @event.SessionType);
        Assert.Equal((int)Weather.LightRain, @event.Weather);
        Assert.Equal(35, @event.TrackTemperature);
        Assert.Equal(22, @event.AirTemperature);
        Assert.Equal(52, @event.TotalLaps);
        Assert.Equal(5891, @event.TrackLength);
        Assert.Equal(3600, @event.SessionTimeLeft);
        Assert.Equal(7200, @event.SessionDuration);
        Assert.Equal((int)SafetyCarType.NoSafetyCar, @event.SafetyCarStatus);
        Assert.Equal(80, @event.PitSpeedLimit);
        Assert.Equal((int)FormulaType.F1Modern, @event.Formula);
        Assert.False(@event.GamePaused);
        Assert.Equal(18, @event.PitStopWindowIdealLap);
        Assert.Equal(25, @event.PitStopWindowLatestLap);
        Assert.Equal(5, @event.PitStopRejoinPosition);
    }

    [Fact]
    public void MapPacket_Session_EmitsSingleEvent()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();
        var packetData = new SessionDataPacket { Header = header };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Single(events);
        Assert.Equal("Session", events[0].EventType);
    }

    // ── Session history tests ──────────────────────────────────────────────────

    [Fact]
    public void MapSessionHistoryData_MapsMetadataFields()
    {
        var mapper = CreateMapper();
        var header = MakeHeader(sessionUid: 7777, frameId: 300);

        // Build the lap history array first — inline arrays on init-only properties
        // can only be populated before passing them into the object initializer.
        var lapHistory = new Array100<LapHistoryData>();
        lapHistory[1] = new LapHistoryData
        {
            LapTimeInMS = 90500,
            Sector1TimeInMS = 28000,
            Sector1TimeMinutes = 0,
            Sector2TimeInMS = 31000,
            Sector2TimeMinutes = 0,
            Sector3TimeInMS = 31500,
            Sector3TimeMinutes = 0,
            LapValidBitFlags = LapValid.LapValid | LapValid.Sector1Valid | LapValid.Sector2Valid | LapValid.Sector3Valid,
        };

        var data = new SessionHistoryDataPacket
        {
            Header = header,
            CarIndex = 0,
            NumLaps = 3, // Current lap is 3 → latest completed is index 1
            NumTyreStints = 1,
            BestLapTimeLapNum = 1,
            BestSector1LapNum = 2,
            BestSector2LapNum = 1,
            BestSector3LapNum = 2,
            LapHistoryData = lapHistory,
        };

        var @event = mapper.MapSessionHistoryData(data, header, DateTimeOffset.UtcNow);

        Assert.Equal("SessionHistory", @event.EventType);
        Assert.Equal("7777", @event.SessionUid);
        Assert.Equal(300u, @event.FrameId);
        Assert.Equal(0, @event.CarIndex);
        Assert.Equal(3, @event.NumLaps);
        Assert.Equal(1, @event.NumTyreStints);
        Assert.Equal(1, @event.BestLapTimeLapNum);
        Assert.Equal(2, @event.BestSector1LapNum);
        Assert.Equal(1, @event.BestSector2LapNum);
        Assert.Equal(2, @event.BestSector3LapNum);
        Assert.Equal(90500, @event.LatestLapTimeMs);
        Assert.Equal(28000, @event.LatestSector1TimeMs);
        Assert.Equal(31000, @event.LatestSector2TimeMs);
        Assert.Equal(31500, @event.LatestSector3TimeMs);
        Assert.True(@event.LatestLapValid);
    }

    [Fact]
    public void MapSessionHistoryData_NoCompletedLaps_NullLatestFields()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();
        var data = new SessionHistoryDataPacket
        {
            Header = header,
            CarIndex = 0,
            NumLaps = 1, // Only current in-progress lap — no completed laps
            NumTyreStints = 1,
        };

        var @event = mapper.MapSessionHistoryData(data, header, DateTimeOffset.UtcNow);

        Assert.Null(@event.LatestLapTimeMs);
        Assert.Null(@event.LatestSector1TimeMs);
        Assert.Null(@event.LatestSector2TimeMs);
        Assert.Null(@event.LatestSector3TimeMs);
        Assert.Null(@event.LatestLapValid);
    }

    [Fact]
    public void MapSessionHistoryData_CombinesSectorTime()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();

        var lapHistory = new Array100<LapHistoryData>();
        lapHistory[1] = new LapHistoryData
        {
            LapTimeInMS = 120000,
            Sector1TimeInMS = 28000,
            Sector1TimeMinutes = 0,
            Sector2TimeInMS = 31000,
            Sector2TimeMinutes = 0,
            Sector3TimeInMS = 500,
            Sector3TimeMinutes = 1, // 1 minute + 500ms = 60500ms
            LapValidBitFlags = LapValid.LapValid,
        };

        var data = new SessionHistoryDataPacket
        {
            Header = header,
            CarIndex = 0,
            NumLaps = 3, // Latest completed = index 1
            NumTyreStints = 1,
            LapHistoryData = lapHistory,
        };

        var @event = mapper.MapSessionHistoryData(data, header, DateTimeOffset.UtcNow);

        Assert.Equal(60500, @event.LatestSector3TimeMs);
    }

    [Fact]
    public void MapPacket_SessionHistory_RespectsPlayerCarFilter()
    {
        var header = MakeHeader(playerCarIndex: 3);

        // CarIndex=5 does not match playerCarIndex=3 → filtered out when allCars=false.
        var packetData = new SessionHistoryDataPacket
        {
            Header = header,
            CarIndex = 5,
            NumLaps = 2,
            NumTyreStints = 1,
        };
        var packet = new UnionPacket(packetData);

        var mapperFiltered = CreateMapper(allCars: false);
        var eventsFiltered = mapperFiltered.MapPacket(packet);
        Assert.Empty(eventsFiltered);

        // With allCars=true, the event should be emitted.
        var mapperAll = CreateMapper(allCars: true);
        var eventsAll = mapperAll.MapPacket(packet);
        Assert.Single(eventsAll);
        Assert.Equal("SessionHistory", eventsAll[0].EventType);
    }

    // ── WeatherForecast tests ──────────────────────────────────────────────────

    [Fact]
    public void MapWeatherForecastSamples_MapsAllFields()
    {
        var mapper = CreateMapper();
        var header = MakeHeader(sessionUid: 1234, frameId: 77, playerCarIndex: 0);
        var timestamp = new DateTimeOffset(2025, 6, 1, 12, 0, 0, TimeSpan.Zero);

        var samples = new Array64<WeatherForecastSample>();
        samples[0] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 10,
            Weather = Weather.LightRain,
            TrackTemperature = 32,
            TrackTemperatureChange = TemperatureChange.Up,
            AirTemperature = 25,
            AirTemperatureChange = TemperatureChange.Down,
            RainPercentage = 60,
        };
        samples[1] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 20,
            Weather = Weather.HeavyRain,
            TrackTemperature = 30,
            TrackTemperatureChange = TemperatureChange.NoChange,
            AirTemperature = 22,
            AirTemperatureChange = TemperatureChange.NoChange,
            RainPercentage = 90,
        };

        var data = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 2,
            WeatherForecastSamples = samples,
            ForecastAccuracy = ForecastAccuracy.Approximate,
        };

        var events = mapper.MapWeatherForecastSamples(data, header, timestamp);

        Assert.Equal(2, events.Count);

        var first = events[0];
        Assert.Equal("WeatherForecast", first.EventType);
        Assert.Equal("1234", first.SessionUid);
        Assert.Equal(77u, first.FrameId);
        Assert.Equal(255, first.CarIndex);
        Assert.Equal(timestamp, first.Timestamp);
        Assert.Equal((int)SessionType.Race, first.ForecastSessionType);
        Assert.Equal(10, first.TimeOffset);
        Assert.Equal((int)Weather.LightRain, first.Weather);
        Assert.Equal(32, first.TrackTemperature);
        Assert.Equal((int)TemperatureChange.Up, first.TrackTemperatureChange);
        Assert.Equal(25, first.AirTemperature);
        Assert.Equal((int)TemperatureChange.Down, first.AirTemperatureChange);
        Assert.Equal(60, first.RainPercentage);
        Assert.Equal(1, first.ForecastAccuracy); // ForecastAccuracy.Approximate = 1
        Assert.Equal(0, first.SampleIndex);

        Assert.Equal(1, events[1].SampleIndex);
    }

    [Fact]
    public void MapWeatherForecastSamples_DeduplicatesIdenticalForecasts()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();
        var timestamp = DateTimeOffset.UtcNow;

        var samples = new Array64<WeatherForecastSample>();
        samples[0] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 5,
            Weather = Weather.Clear,
            TrackTemperature = 40,
            TrackTemperatureChange = TemperatureChange.NoChange,
            AirTemperature = 30,
            AirTemperatureChange = TemperatureChange.NoChange,
            RainPercentage = 0,
        };
        var data = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 1,
            WeatherForecastSamples = samples,
            ForecastAccuracy = ForecastAccuracy.Perfect,
        };

        var firstCall = mapper.MapWeatherForecastSamples(data, header, timestamp);
        var secondCall = mapper.MapWeatherForecastSamples(data, header, timestamp);

        Assert.Single(firstCall);
        Assert.Empty(secondCall);
    }

    [Fact]
    public void MapWeatherForecastSamples_EmitsOnChange()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();
        var timestamp = DateTimeOffset.UtcNow;

        var samplesA = new Array64<WeatherForecastSample>();
        samplesA[0] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 5,
            Weather = Weather.Clear,
            TrackTemperature = 40,
            TrackTemperatureChange = TemperatureChange.NoChange,
            AirTemperature = 30,
            AirTemperatureChange = TemperatureChange.NoChange,
            RainPercentage = 0,
        };
        var dataA = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 1,
            WeatherForecastSamples = samplesA,
            ForecastAccuracy = ForecastAccuracy.Perfect,
        };

        var samplesB = new Array64<WeatherForecastSample>();
        samplesB[0] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 5,
            Weather = Weather.LightRain, // Different weather — forecast has changed.
            TrackTemperature = 40,
            TrackTemperatureChange = TemperatureChange.NoChange,
            AirTemperature = 30,
            AirTemperatureChange = TemperatureChange.NoChange,
            RainPercentage = 30,
        };
        var dataB = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 1,
            WeatherForecastSamples = samplesB,
            ForecastAccuracy = ForecastAccuracy.Perfect,
        };

        var firstCall = mapper.MapWeatherForecastSamples(dataA, header, timestamp);
        var secondCall = mapper.MapWeatherForecastSamples(dataB, header, timestamp);

        Assert.Single(firstCall);
        Assert.Single(secondCall);
        Assert.Equal((int)Weather.LightRain, secondCall[0].Weather);
    }

    [Fact]
    public void MapWeatherForecastSamples_EmitsOnForecastAccuracyChange()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();
        var timestamp = DateTimeOffset.UtcNow;

        var samples = new Array64<WeatherForecastSample>();
        samples[0] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 5,
            Weather = Weather.Clear,
            TrackTemperature = 40,
            TrackTemperatureChange = TemperatureChange.NoChange,
            AirTemperature = 30,
            AirTemperatureChange = TemperatureChange.NoChange,
            RainPercentage = 0,
        };

        var dataA = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 1,
            WeatherForecastSamples = samples,
            ForecastAccuracy = ForecastAccuracy.Perfect,
        };
        var dataB = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 1,
            WeatherForecastSamples = samples,
            ForecastAccuracy = ForecastAccuracy.Approximate, // Only accuracy changed
        };

        var firstCall = mapper.MapWeatherForecastSamples(dataA, header, timestamp);
        var secondCall = mapper.MapWeatherForecastSamples(dataB, header, timestamp);

        Assert.Single(firstCall);
        Assert.Single(secondCall);
        Assert.Equal((int)ForecastAccuracy.Approximate, secondCall[0].ForecastAccuracy);
    }

    [Fact]
    public void MapWeatherForecastSamples_EmptyWhenZeroSamples()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();
        var data = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 0,
        };

        var events = mapper.MapWeatherForecastSamples(data, header, DateTimeOffset.UtcNow);

        Assert.Empty(events);
    }

    [Fact]
    public void MapPacket_Session_IncludesForecastEvents()
    {
        var mapper = CreateMapper();
        var header = MakeHeader();

        var samples = new Array64<WeatherForecastSample>();
        samples[0] = new WeatherForecastSample
        {
            SessionType = SessionType.Race,
            TimeOffset = 5,
            Weather = Weather.Clear,
            TrackTemperature = 38,
            TrackTemperatureChange = TemperatureChange.NoChange,
            AirTemperature = 28,
            AirTemperatureChange = TemperatureChange.NoChange,
            RainPercentage = 0,
        };
        var packetData = new SessionDataPacket
        {
            Header = header,
            NumWeatherForecastSamples = 1,
            WeatherForecastSamples = samples,
            ForecastAccuracy = ForecastAccuracy.Perfect,
        };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Equal(2, events.Count);
        Assert.Equal("Session", events[0].EventType);
        Assert.Equal("WeatherForecast", events[1].EventType);
    }

    // ── CarMotion tests ────────────────────────────────────────────────────────

    [Fact]
    public void MapCarMotionData_MapsGForceFields()
    {
        var mapper = CreateMapper();
        var data = new CarMotionData
        {
            GForceLateral = 2.5f,
            GForceLongitudinal = -1.8f,
            GForceVertical = 1.0f,
        };
        var header = MakeHeader(sessionUid: 4444, frameId: 150, playerCarIndex: 2);

        var @event = mapper.MapCarMotionData(data, header, 2, DateTimeOffset.UtcNow);

        Assert.Equal("CarMotion", @event.EventType);
        Assert.Equal("4444", @event.SessionUid);
        Assert.Equal(150u, @event.FrameId);
        Assert.Equal(2, @event.CarIndex);
        Assert.Equal(2.5f, @event.GForceLateral);
        Assert.Equal(-1.8f, @event.GForceLongitudinal);
        Assert.Equal(1.0f, @event.GForceVertical);
    }

    [Fact]
    public void MapPacket_Motion_PlayerCarOnly_WhenAllCarsFalse()
    {
        var mapper = CreateMapper(allCars: false);
        var header = MakeHeader(playerCarIndex: 7);
        var packetData = new MotionDataPacket { Header = header };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Single(events);
        Assert.Equal("CarMotion", events[0].EventType);
        Assert.Equal(7, events[0].CarIndex);
    }

    [Fact]
    public void MapPacket_Motion_AllCars_WhenAllCarsTrue()
    {
        var mapper = CreateMapper(allCars: true);
        var header = MakeHeader(playerCarIndex: 0);
        var packetData = new MotionDataPacket { Header = header };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        Assert.Equal(20, events.Count);
        Assert.All(events, e => Assert.Equal("CarMotion", e.EventType));
    }

    // ── FinalClassification tests ──────────────────────────────────────────────

    [Fact]
    public void MapFinalClassificationData_MapsAllFields()
    {
        var mapper = CreateMapper();

        var tyreStintsVisual = new Array8<VisualCompound>();
        tyreStintsVisual[0] = VisualCompound.F1Soft;
        tyreStintsVisual[1] = VisualCompound.F1Hard;

        var tyreStintsEndLaps = new Array8<byte>();
        tyreStintsEndLaps[0] = 15;
        tyreStintsEndLaps[1] = 52;

        var data = new FinalClassificationData
        {
            Position = 1,
            NumLaps = 52,
            GridPosition = 3,
            Points = 25,
            NumPitStops = 1,
            ResultStatus = ResultStatus.Finished,
            ResultReason = ResultReason.Invalid,
            BestLapTimeInMS = 90500,
            TotalRaceTime = 5432.123,
            PenaltiesTime = 0,
            NumPenalties = 0,
            NumTyreStints = 2,
            TyreStintsVisual = tyreStintsVisual,
            TyreStintsEndLaps = tyreStintsEndLaps,
        };
        var header = MakeHeader(sessionUid: 6666, frameId: 500, playerCarIndex: 0);

        var @event = mapper.MapFinalClassificationData(data, header, 0, DateTimeOffset.UtcNow);

        Assert.Equal("FinalClassification", @event.EventType);
        Assert.Equal("6666", @event.SessionUid);
        Assert.Equal(500u, @event.FrameId);
        Assert.Equal(0, @event.CarIndex);
        Assert.Equal(1, @event.Position);
        Assert.Equal(52, @event.NumLaps);
        Assert.Equal(3, @event.GridPosition);
        Assert.Equal(25, @event.Points);
        Assert.Equal(1, @event.NumPitStops);
        Assert.Equal((int)ResultStatus.Finished, @event.ResultStatus);
        Assert.Equal((int)ResultReason.Invalid, @event.ResultReason);
        Assert.Equal(90500, @event.BestLapTimeMs);
        Assert.Equal(5432.123, @event.TotalRaceTime);
        Assert.Equal(0, @event.PenaltiesTime);
        Assert.Equal(0, @event.NumPenalties);
        Assert.Equal(2, @event.NumTyreStints);
        Assert.Contains(",", @event.TyreStintsVisual);
        Assert.Contains(",", @event.TyreStintsEndLaps);
        Assert.Equal("15,52", @event.TyreStintsEndLaps);
    }

    [Fact]
    public void MapPacket_FinalClassification_AlwaysEmitsAllCars()
    {
        var mapper = CreateMapper(allCars: false);
        var header = MakeHeader(playerCarIndex: 3);
        var packetData = new FinalClassificationDataPacket { Header = header, NumCars = 10 };
        var packet = new UnionPacket(packetData);

        var events = mapper.MapPacket(packet);

        // FinalClassification always emits all cars regardless of AllCars setting
        Assert.Equal(10, events.Count);
        Assert.All(events, e => Assert.Equal("FinalClassification", e.EventType));
        for (byte i = 0; i < 10; i++)
            Assert.Equal(i, events[i].CarIndex);
    }

    [Fact]
    public void MapFinalClassificationData_ZeroStints_EmptyStrings()
    {
        var mapper = CreateMapper();
        var data = new FinalClassificationData { NumTyreStints = 0 };
        var header = MakeHeader();

        var @event = mapper.MapFinalClassificationData(data, header, 0, DateTimeOffset.UtcNow);

        Assert.Equal("", @event.TyreStintsVisual);
        Assert.Equal("", @event.TyreStintsEndLaps);
    }
}
