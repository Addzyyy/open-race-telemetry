using F1Game.UDP.Data;
using F1Game.UDP.Enums;
using F1Game.UDP.Packets;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;

namespace TelemetryIngester.Mapping;

/// <summary>
/// Translates raw F1Game.UDP packet structs into canonical <see cref="TelemetryEvent"/> records.
/// This is the only place in the codebase that depends on the F1Game.UDP library types —
/// everything downstream works with our own event records instead.
/// </summary>
public sealed class PacketMapper(IOptions<TelemetryOptions> options) : IPacketMapper
{
    private readonly TelemetryOptions _options = options.Value;
    private int? _lastForecastHash;

    /// <summary>
    /// Inspects the incoming packet, maps it to zero or more events, and returns them.
    /// Seven packet types are handled; all others (motion, car setup, etc.) return an empty list.
    /// </summary>
    public IReadOnlyList<TelemetryEvent> MapPacket(UnionPacket packet)
    {
        var header = packet.Header;
        var now = DateTimeOffset.UtcNow;
        List<TelemetryEvent> events = [];

        // Each TryGet call checks whether this packet is of the given type.
        // Exactly one will succeed per packet — the game sends one type per UDP datagram.
        if (packet.TryGetCarTelemetryDataPacket(out var carTelemetry))
        {
            foreach (var carIndex in CarIndices(header))
                events.Add(MapCarTelemetryData(carTelemetry.CarTelemetryData[carIndex], header, carIndex, now));
            return events;
        }

        if (packet.TryGetLapDataPacket(out var lapData))
        {
            foreach (var carIndex in CarIndices(header))
                events.Add(MapLapData(lapData.LapData[carIndex], header, carIndex, now));
            return events;
        }

        if (packet.TryGetCarStatusDataPacket(out var carStatus))
        {
            foreach (var carIndex in CarIndices(header))
                events.Add(MapCarStatusData(carStatus.CarStatusData[carIndex], header, carIndex, now));
            return events;
        }

        if (packet.TryGetCarDamageDataPacket(out var carDamage))
        {
            foreach (var carIndex in CarIndices(header))
                events.Add(MapCarDamageData(carDamage.CarDamageData[carIndex], header, carIndex, now));
            return events;
        }

        if (packet.TryGetParticipantsDataPacket(out var participants))
        {
            // Always emit all active cars — participant names are session metadata.
            var count = Math.Min(participants.NumActiveCars, (byte)20);
            for (byte i = 0; i < count; i++)
                events.Add(MapParticipantData(participants.Participants[i], header, i, now));
            return events;
        }

        if (packet.TryGetSessionDataPacket(out var session))
        {
            events.Add(MapSessionData(session, header, now));
            events.AddRange(MapWeatherForecastSamples(session, header, now));
            return events;
        }

        if (packet.TryGetSessionHistoryDataPacket(out var history))
        {
            // Use the packet body's CarIndex (not the header's PlayerCarIndex) for filtering.
            if (_options.AllCars || history.CarIndex == header.PlayerCarIndex)
                events.Add(MapSessionHistoryData(history, header, now));
            return events;
        }

        return events;
    }

    // Lazily builds and caches the full 0–19 index list the first time AllCars mode is used.
    // The field keyword (C# 14) gives us a compiler-generated backing field without a separate declaration.
    private IReadOnlyList<byte> AllCarIndices
    {
        get => field ??= Enumerable.Range(0, 20).Select(i => (byte)i).ToArray();
    }

    /// <summary>
    /// Returns the car indices to emit events for.
    /// In default mode only the player's own car is emitted; in AllCars mode all 20 slots are emitted.
    /// </summary>
    private IEnumerable<byte> CarIndices(PacketHeader header) =>
        _options.AllCars ? AllCarIndices : [(byte)header.PlayerCarIndex];

    // ── Internal mapping methods ───────────────────────────────────────────────
    // Marked internal so unit tests can call them directly via InternalsVisibleTo,
    // without needing to construct a full UnionPacket.

    /// <summary>Maps a single car's telemetry struct to a <see cref="CarTelemetryEvent"/>.</summary>
    internal CarTelemetryEvent MapCarTelemetryData(
        CarTelemetryData data, PacketHeader header, byte carIndex, DateTimeOffset timestamp) =>
        new()
        {
            EventType = "CarTelemetry",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = carIndex,
            Speed = data.Speed,
            Throttle = data.Throttle,
            Steer = data.Steer,
            Brake = data.Brake,
            Clutch = data.Clutch,
            Gear = data.Gear,
            EngineRpm = data.EngineRPM,
            Drs = data.IsDrsOn,
            RevLightsPercent = data.RevLightsPercent,
            // F1Game.UDP uses a Tyres<T> struct with named positions rather than a plain array.
            // RearLeft/RearRight/FrontLeft/FrontRight correspond to Rl/Rr/Fl/Fr in our event names.
            BrakesTempRl = data.BrakesTemperature.RearLeft,
            BrakesTempRr = data.BrakesTemperature.RearRight,
            BrakesTempFl = data.BrakesTemperature.FrontLeft,
            BrakesTempFr = data.BrakesTemperature.FrontRight,
            TyresSurfaceTempRl = data.TyresSurfaceTemperature.RearLeft,
            TyresSurfaceTempRr = data.TyresSurfaceTemperature.RearRight,
            TyresSurfaceTempFl = data.TyresSurfaceTemperature.FrontLeft,
            TyresSurfaceTempFr = data.TyresSurfaceTemperature.FrontRight,
            TyresInnerTempRl = data.TyresInnerTemperature.RearLeft,
            TyresInnerTempRr = data.TyresInnerTemperature.RearRight,
            TyresInnerTempFl = data.TyresInnerTemperature.FrontLeft,
            TyresInnerTempFr = data.TyresInnerTemperature.FrontRight,
        };

    /// <summary>Maps a single car's lap data struct to a <see cref="LapDataEvent"/>.</summary>
    internal LapDataEvent MapLapData(
        LapData data, PacketHeader header, byte carIndex, DateTimeOffset timestamp) =>
        new()
        {
            EventType = "LapData",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = carIndex,
            CurrentLapTimeMs = (int)data.CurrentLapTimeInMS,
            CurrentLapNum = data.CurrentLapNum,
            Sector1TimeMs = CombineSectorTime(data.Sector1TimeInMS, data.Sector1TimeInMinutes),
            Sector2TimeMs = CombineSectorTime(data.Sector2TimeInMS, data.Sector2TimeInMinutes),
            Sector3TimeMs = null, // Not available in LapData; derived from LapHistory packets (future work)
            LapDistance = data.LapDistance,
            TotalDistance = data.TotalDistance,
            CarPosition = data.CarPosition,
            CurrentLapInvalid = data.IsCurrentLapInvalid,
            Penalties = data.Penalties,
            NumPitStops = data.NumPitStops,
            PitStatus = (int)data.PitStatus,
            Sector = (int)data.Sector,
            ResultStatus = (int)data.ResultStatus,
        };

    /// <summary>
    /// Maps a car's status data (fuel, ERS, tyres) to a <see cref="CarStatusEvent"/>.
    /// Tyre wear fields are left null — those come from <see cref="MapCarDamageData"/>.
    /// </summary>
    internal CarStatusEvent MapCarStatusData(
        CarStatusData data, PacketHeader header, byte carIndex, DateTimeOffset timestamp) =>
        new()
        {
            EventType = "CarStatus",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = carIndex,
            TyreWearRl = null,
            TyreWearRr = null,
            TyreWearFl = null,
            TyreWearFr = null,
            ActualTyreCompound = (int)data.ActualTyreCompound,
            VisualTyreCompound = (int)data.VisualTyreCompound,
            TyresAgeLaps = data.TyresAgeLaps,
            FuelInTank = data.FuelInTank,
            FuelCapacity = data.FuelCapacity,
            FuelRemainingLaps = data.FuelRemainingLaps,
            ErsStoreEnergy = data.ErsStoreEnergy,
            ErsDeployMode = (int)data.ErsDeployMode,
            ErsHarvestedLapMguk = data.ErsHarvestedThisLapMGUK,
            ErsHarvestedLapMguh = data.ErsHarvestedThisLapMGUH,
            ErsDeployedLap = data.ErsDeployedThisLap,
        };

    /// <summary>
    /// Maps a car's damage data (tyre wear) to a <see cref="CarStatusEvent"/>.
    /// Fuel and ERS fields are left null — those come from <see cref="MapCarStatusData"/>.
    /// </summary>
    internal CarStatusEvent MapCarDamageData(
        CarDamageData data, PacketHeader header, byte carIndex, DateTimeOffset timestamp) =>
        new()
        {
            EventType = "CarStatus",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = carIndex,
            TyreWearRl = data.TyresWear.RearLeft,
            TyreWearRr = data.TyresWear.RearRight,
            TyreWearFl = data.TyresWear.FrontLeft,
            TyreWearFr = data.TyresWear.FrontRight,
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

    /// <summary>Maps a single participant's data to a <see cref="ParticipantEvent"/>.</summary>
    internal ParticipantEvent MapParticipantData(
        ParticipantData data, PacketHeader header, byte carIndex, DateTimeOffset timestamp) =>
        new()
        {
            EventType = "Participant",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = carIndex,
            Name = data.Name,
            Team = (int)data.Team,
            RaceNumber = data.RaceNumber,
            Nationality = (int)data.Nationality,
            IsAiControlled = data.IsAiControlled,
            Driver = (int)data.Driver,
            Platform = (int)data.Platform,
            IsMyTeam = data.IsMyTeam,
            IsTelemetryPublic = data.IsTelemetryPublic,
        };

    /// <summary>Maps a session data packet to a <see cref="SessionEvent"/>.</summary>
    internal SessionEvent MapSessionData(
        SessionDataPacket data, PacketHeader header, DateTimeOffset timestamp) =>
        new()
        {
            EventType = "Session",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = 255, // Sentinel — session data is not per-car.
            Track = (int)data.Track,
            SessionType = (int)data.SessionType,
            Weather = (int)data.Weather,
            TrackTemperature = data.TrackTemperature,
            AirTemperature = data.AirTemperature,
            TotalLaps = data.TotalLaps,
            TrackLength = data.TrackLength,
            SessionTimeLeft = data.SessionTimeLeft,
            SessionDuration = data.SessionDuration,
            SafetyCarStatus = (int)data.SafetyCarStatus,
            PitSpeedLimit = data.PitSpeedLimit,
            Formula = (int)data.Formula,
            GamePaused = data.GamePaused,
            PitStopWindowIdealLap = data.PitStopWindowIdealLap,
            PitStopWindowLatestLap = data.PitStopWindowLatestLap,
            PitStopRejoinPosition = data.PitStopRejoinPosition,
        };

    /// <summary>Maps a session history packet to a <see cref="SessionHistoryEvent"/>.</summary>
    internal SessionHistoryEvent MapSessionHistoryData(
        SessionHistoryDataPacket data, PacketHeader header, DateTimeOffset timestamp)
    {
        // NumLaps includes the current in-progress lap, so the latest completed lap
        // is at index NumLaps - 2. If NumLaps <= 1, there are no completed laps.
        int? latestLapTimeMs = null;
        int? latestSector1TimeMs = null;
        int? latestSector2TimeMs = null;
        int? latestSector3TimeMs = null;
        bool? latestLapValid = null;

        if (data.NumLaps > 1)
        {
            var lap = data.LapHistoryData[data.NumLaps - 2];
            latestLapTimeMs = lap.LapTimeInMS > 0 ? (int)lap.LapTimeInMS : null;
            latestSector1TimeMs = CombineSectorTime(lap.Sector1TimeInMS, lap.Sector1TimeMinutes);
            latestSector2TimeMs = CombineSectorTime(lap.Sector2TimeInMS, lap.Sector2TimeMinutes);
            latestSector3TimeMs = CombineSectorTime(lap.Sector3TimeInMS, lap.Sector3TimeMinutes);
            latestLapValid = (lap.LapValidBitFlags & LapValid.LapValid) != 0;
        }

        return new()
        {
            EventType = "SessionHistory",
            SessionUid = header.SessionUID.ToString(),
            Timestamp = timestamp,
            FrameId = header.FrameIdentifier,
            CarIndex = data.CarIndex,
            NumLaps = data.NumLaps,
            NumTyreStints = data.NumTyreStints,
            BestLapTimeLapNum = data.BestLapTimeLapNum,
            BestSector1LapNum = data.BestSector1LapNum,
            BestSector2LapNum = data.BestSector2LapNum,
            BestSector3LapNum = data.BestSector3LapNum,
            LatestLapTimeMs = latestLapTimeMs,
            LatestSector1TimeMs = latestSector1TimeMs,
            LatestSector2TimeMs = latestSector2TimeMs,
            LatestSector3TimeMs = latestSector3TimeMs,
            LatestLapValid = latestLapValid,
        };
    }

    /// <summary>
    /// Extracts weather forecast samples from a session packet, using hash-based deduplication
    /// to suppress duplicate writes when the forecast hasn't changed (~2x/sec → only on change).
    /// </summary>
    internal IReadOnlyList<WeatherForecastEvent> MapWeatherForecastSamples(
        SessionDataPacket data, PacketHeader header, DateTimeOffset timestamp)
    {
        var count = Math.Min(data.NumWeatherForecastSamples, (byte)64);
        if (count == 0)
            return [];

        var hash = ComputeForecastHash(data, count);
        if (hash == _lastForecastHash)
            return [];

        _lastForecastHash = hash;

        var forecastAccuracy = (int)data.ForecastAccuracy;
        var sessionUid = header.SessionUID.ToString();
        var frameId = header.FrameIdentifier;
        var events = new List<WeatherForecastEvent>(count);

        for (int i = 0; i < count; i++)
        {
            var sample = data.WeatherForecastSamples[i];
            events.Add(new WeatherForecastEvent
            {
                EventType = "WeatherForecast",
                SessionUid = sessionUid,
                Timestamp = timestamp,
                FrameId = frameId,
                CarIndex = 255,
                ForecastSessionType = (int)sample.SessionType,
                TimeOffset = sample.TimeOffset,
                Weather = (int)sample.Weather,
                TrackTemperature = sample.TrackTemperature,
                TrackTemperatureChange = (int)sample.TrackTemperatureChange,
                AirTemperature = sample.AirTemperature,
                AirTemperatureChange = (int)sample.AirTemperatureChange,
                RainPercentage = sample.RainPercentage,
                ForecastAccuracy = forecastAccuracy,
                SampleIndex = i,
            });
        }

        return events;
    }

    private static int ComputeForecastHash(SessionDataPacket data, int count)
    {
        var hash = new HashCode();
        hash.Add(count);
        for (int i = 0; i < count; i++)
        {
            var s = data.WeatherForecastSamples[i];
            hash.Add((int)s.SessionType);
            hash.Add(s.TimeOffset);
            hash.Add((int)s.Weather);
            hash.Add(s.TrackTemperature);
            hash.Add((int)s.TrackTemperatureChange);
            hash.Add(s.AirTemperature);
            hash.Add((int)s.AirTemperatureChange);
            hash.Add(s.RainPercentage);
        }
        return hash.ToHashCode();
    }

    /// <summary>
    /// Combines the split minutes/milliseconds fields the F1 game uses for sector times
    /// into a single total-milliseconds value, or returns <c>null</c> if the sector hasn't
    /// been completed yet (both parts are zero).
    /// </summary>
    private static int? CombineSectorTime(ushort ms, byte minutes) =>
        ms == 0 && minutes == 0 ? null : (int?)(minutes * 60_000 + ms);
}
