using F1Game.UDP.Data;
using F1Game.UDP.Packets;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;

namespace TelemetryIngester.Mapping;

public sealed class PacketMapper(IOptions<TelemetryOptions> options) : IPacketMapper
{
    private readonly TelemetryOptions _options = options.Value;

    public IReadOnlyList<TelemetryEvent> MapPacket(UnionPacket packet)
    {
        var header = packet.Header;
        var now = DateTimeOffset.UtcNow;
        List<TelemetryEvent> events = [];

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

        return events;
    }

    private IReadOnlyList<byte> AllCarIndices
    {
        get => field ??= Enumerable.Range(0, 20).Select(i => (byte)i).ToArray();
    }

    private IEnumerable<byte> CarIndices(PacketHeader header) =>
        _options.AllCars ? AllCarIndices : [(byte)header.PlayerCarIndex];

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

    private static int? CombineSectorTime(ushort ms, byte minutes) =>
        ms == 0 && minutes == 0 ? null : (int?)(minutes * 60_000 + ms);
}
