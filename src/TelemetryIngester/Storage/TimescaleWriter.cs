using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;

namespace TelemetryIngester.Storage;

public sealed class TimescaleWriter(
    IOptions<TimescaleDbOptions> options,
    ILogger<TimescaleWriter> logger) : ITimescaleWriter
{
    private readonly string _connectionString = options.Value.ConnectionString;

    public async Task WriteBatchAsync(IReadOnlyList<TelemetryEvent> events, CancellationToken ct)
    {
        if (events.Count == 0)
            return;

        var carTelemetry = new List<CarTelemetryEvent>();
        var lapData = new List<LapDataEvent>();
        var carStatus = new List<CarStatusEvent>();

        foreach (var e in events)
        {
            switch (e)
            {
                case CarTelemetryEvent cte: carTelemetry.Add(cte); break;
                case LapDataEvent lde: lapData.Add(lde); break;
                case CarStatusEvent cse: carStatus.Add(cse); break;
            }
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        if (carTelemetry.Count > 0) await WriteCarTelemetryAsync(conn, carTelemetry, ct);
        if (lapData.Count > 0) await WriteLapDataAsync(conn, lapData, ct);
        if (carStatus.Count > 0) await WriteCarStatusAsync(conn, carStatus, ct);

        logger.LogDebug("Wrote batch of {Count} events ({Telemetry} telemetry, {Lap} lap, {Status} status)",
            events.Count, carTelemetry.Count, lapData.Count, carStatus.Count);
    }

    private static async Task WriteCarTelemetryAsync(NpgsqlConnection conn, List<CarTelemetryEvent> events, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY car_telemetry (time, session_uid, frame_id, car_index, " +
            "speed, throttle, steer, brake, clutch, gear, engine_rpm, drs, rev_lights_percent, " +
            "brakes_temp_rl, brakes_temp_rr, brakes_temp_fl, brakes_temp_fr, " +
            "tyres_surface_temp_rl, tyres_surface_temp_rr, tyres_surface_temp_fl, tyres_surface_temp_fr, " +
            "tyres_inner_temp_rl, tyres_inner_temp_rr, tyres_inner_temp_fl, tyres_inner_temp_fr) " +
            "FROM STDIN (FORMAT BINARY)", ct);

        foreach (var e in events)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(e.Timestamp, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(e.SessionUid, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((int)e.FrameId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CarIndex, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Speed, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.Throttle, NpgsqlDbType.Real, ct);
            await writer.WriteAsync(e.Steer, NpgsqlDbType.Real, ct);
            await writer.WriteAsync(e.Brake, NpgsqlDbType.Real, ct);
            await writer.WriteAsync((short)e.Clutch, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Gear, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.EngineRpm, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.Drs, NpgsqlDbType.Boolean, ct);
            await writer.WriteAsync((short)e.RevLightsPercent, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BrakesTempRl, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BrakesTempRr, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BrakesTempFl, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BrakesTempFr, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresSurfaceTempRl, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresSurfaceTempRr, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresSurfaceTempFl, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresSurfaceTempFr, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresInnerTempRl, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresInnerTempRr, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresInnerTempFl, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TyresInnerTempFr, NpgsqlDbType.Smallint, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task WriteLapDataAsync(NpgsqlConnection conn, List<LapDataEvent> events, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY lap_data (time, session_uid, frame_id, car_index, " +
            "current_lap_time_ms, current_lap_num, sector1_time_ms, sector2_time_ms, sector3_time_ms, " +
            "lap_distance, total_distance, car_position, current_lap_invalid, " +
            "penalties, num_pit_stops, pit_status, sector, result_status) " +
            "FROM STDIN (FORMAT BINARY)", ct);

        foreach (var e in events)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(e.Timestamp, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(e.SessionUid, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((int)e.FrameId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CarIndex, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.CurrentLapTimeMs, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CurrentLapNum, NpgsqlDbType.Smallint, ct);
            await WriteNullableIntAsync(writer, e.Sector1TimeMs, NpgsqlDbType.Integer, ct);
            await WriteNullableIntAsync(writer, e.Sector2TimeMs, NpgsqlDbType.Integer, ct);
            await WriteNullableIntAsync(writer, e.Sector3TimeMs, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(e.LapDistance, NpgsqlDbType.Real, ct);
            await writer.WriteAsync(e.TotalDistance, NpgsqlDbType.Real, ct);
            await writer.WriteAsync((short)e.CarPosition, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.CurrentLapInvalid, NpgsqlDbType.Boolean, ct);
            await writer.WriteAsync((short)e.Penalties, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.NumPitStops, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.PitStatus, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Sector, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.ResultStatus, NpgsqlDbType.Smallint, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task WriteCarStatusAsync(NpgsqlConnection conn, List<CarStatusEvent> events, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY car_status (time, session_uid, frame_id, car_index, " +
            "tyre_wear_rl, tyre_wear_rr, tyre_wear_fl, tyre_wear_fr, " +
            "actual_tyre_compound, visual_tyre_compound, tyres_age_laps, " +
            "fuel_in_tank, fuel_capacity, fuel_remaining_laps, " +
            "ers_store_energy, ers_deploy_mode, ers_harvested_lap_mguk, ers_harvested_lap_mguh, ers_deployed_lap) " +
            "FROM STDIN (FORMAT BINARY)", ct);

        foreach (var e in events)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(e.Timestamp, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(e.SessionUid, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((int)e.FrameId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CarIndex, NpgsqlDbType.Smallint, ct);
            await WriteNullableFloatAsync(writer, e.TyreWearRl, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.TyreWearRr, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.TyreWearFl, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.TyreWearFr, NpgsqlDbType.Real, ct);
            await WriteNullableShortAsync(writer, e.ActualTyreCompound, NpgsqlDbType.Smallint, ct);
            await WriteNullableShortAsync(writer, e.VisualTyreCompound, NpgsqlDbType.Smallint, ct);
            await WriteNullableShortAsync(writer, e.TyresAgeLaps, NpgsqlDbType.Smallint, ct);
            await WriteNullableFloatAsync(writer, e.FuelInTank, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.FuelCapacity, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.FuelRemainingLaps, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.ErsStoreEnergy, NpgsqlDbType.Real, ct);
            await WriteNullableShortAsync(writer, e.ErsDeployMode, NpgsqlDbType.Smallint, ct);
            await WriteNullableFloatAsync(writer, e.ErsHarvestedLapMguk, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.ErsHarvestedLapMguh, NpgsqlDbType.Real, ct);
            await WriteNullableFloatAsync(writer, e.ErsDeployedLap, NpgsqlDbType.Real, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task WriteNullableIntAsync(NpgsqlBinaryImporter writer, int? value, NpgsqlDbType dbType, CancellationToken ct)
    {
        if (value.HasValue)
            await writer.WriteAsync(value.Value, dbType, ct);
        else
            await writer.WriteNullAsync(ct);
    }

    private static async Task WriteNullableFloatAsync(NpgsqlBinaryImporter writer, float? value, NpgsqlDbType dbType, CancellationToken ct)
    {
        if (value.HasValue)
            await writer.WriteAsync(value.Value, dbType, ct);
        else
            await writer.WriteNullAsync(ct);
    }

    private static async Task WriteNullableShortAsync(NpgsqlBinaryImporter writer, int? value, NpgsqlDbType dbType, CancellationToken ct)
    {
        if (value.HasValue)
            await writer.WriteAsync((short)value.Value, dbType, ct);
        else
            await writer.WriteNullAsync(ct);
    }
}
