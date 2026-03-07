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
        var participants = new List<ParticipantEvent>();
        var session = new List<SessionEvent>();
        var sessionHistory = new List<SessionHistoryEvent>();

        foreach (var e in events)
        {
            switch (e)
            {
                case CarTelemetryEvent cte: carTelemetry.Add(cte); break;
                case LapDataEvent lde: lapData.Add(lde); break;
                case CarStatusEvent cse: carStatus.Add(cse); break;
                case ParticipantEvent pe: participants.Add(pe); break;
                case SessionEvent se: session.Add(se); break;
                case SessionHistoryEvent she: sessionHistory.Add(she); break;
            }
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        if (carTelemetry.Count > 0) await WriteCarTelemetryAsync(conn, carTelemetry, ct);
        if (lapData.Count > 0) await WriteLapDataAsync(conn, lapData, ct);
        if (carStatus.Count > 0) await WriteCarStatusAsync(conn, carStatus, ct);
        if (participants.Count > 0) await WriteParticipantsAsync(conn, participants, ct);
        if (session.Count > 0) await WriteSessionAsync(conn, session, ct);
        if (sessionHistory.Count > 0) await WriteSessionHistoryAsync(conn, sessionHistory, ct);

        logger.LogDebug("Wrote batch of {Count} events ({Telemetry} telemetry, {Lap} lap, {Status} status, {Participants} participants, {Session} session, {History} history)",
            events.Count, carTelemetry.Count, lapData.Count, carStatus.Count, participants.Count, session.Count, sessionHistory.Count);
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

    private static async Task WriteParticipantsAsync(NpgsqlConnection conn, List<ParticipantEvent> events, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY participants (time, session_uid, frame_id, car_index, " +
            "name, team, race_number, nationality, is_ai_controlled, " +
            "driver, platform, is_my_team, is_telemetry_public) " +
            "FROM STDIN (FORMAT BINARY)", ct);

        foreach (var e in events)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(e.Timestamp, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(e.SessionUid, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((int)e.FrameId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CarIndex, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.Name, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((short)e.Team, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.RaceNumber, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Nationality, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.IsAiControlled, NpgsqlDbType.Boolean, ct);
            await writer.WriteAsync((short)e.Driver, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Platform, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.IsMyTeam, NpgsqlDbType.Boolean, ct);
            await writer.WriteAsync(e.IsTelemetryPublic, NpgsqlDbType.Boolean, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task WriteSessionAsync(NpgsqlConnection conn, List<SessionEvent> events, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY session (time, session_uid, frame_id, car_index, " +
            "track, session_type, weather, track_temperature, air_temperature, " +
            "total_laps, track_length, session_time_left, session_duration, " +
            "safety_car_status, pit_speed_limit, formula, game_paused, " +
            "pit_stop_window_ideal_lap, pit_stop_window_latest_lap, pit_stop_rejoin_position) " +
            "FROM STDIN (FORMAT BINARY)", ct);

        foreach (var e in events)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(e.Timestamp, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(e.SessionUid, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((int)e.FrameId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CarIndex, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Track, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.SessionType, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Weather, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TrackTemperature, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.AirTemperature, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.TotalLaps, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.TrackLength, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(e.SessionTimeLeft, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(e.SessionDuration, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.SafetyCarStatus, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.PitSpeedLimit, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.Formula, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(e.GamePaused, NpgsqlDbType.Boolean, ct);
            await writer.WriteAsync((short)e.PitStopWindowIdealLap, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.PitStopWindowLatestLap, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.PitStopRejoinPosition, NpgsqlDbType.Smallint, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task WriteSessionHistoryAsync(NpgsqlConnection conn, List<SessionHistoryEvent> events, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY session_history (time, session_uid, frame_id, car_index, " +
            "num_laps, num_tyre_stints, best_lap_time_lap_num, " +
            "best_sector1_lap_num, best_sector2_lap_num, best_sector3_lap_num, " +
            "latest_lap_time_ms, latest_sector1_time_ms, latest_sector2_time_ms, latest_sector3_time_ms, " +
            "latest_lap_valid) " +
            "FROM STDIN (FORMAT BINARY)", ct);

        foreach (var e in events)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(e.Timestamp, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(e.SessionUid, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((int)e.FrameId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)e.CarIndex, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.NumLaps, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.NumTyreStints, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BestLapTimeLapNum, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BestSector1LapNum, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BestSector2LapNum, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync((short)e.BestSector3LapNum, NpgsqlDbType.Smallint, ct);
            await WriteNullableIntAsync(writer, e.LatestLapTimeMs, NpgsqlDbType.Integer, ct);
            await WriteNullableIntAsync(writer, e.LatestSector1TimeMs, NpgsqlDbType.Integer, ct);
            await WriteNullableIntAsync(writer, e.LatestSector2TimeMs, NpgsqlDbType.Integer, ct);
            await WriteNullableIntAsync(writer, e.LatestSector3TimeMs, NpgsqlDbType.Integer, ct);
            await WriteNullableBoolAsync(writer, e.LatestLapValid, ct);
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

    private static async Task WriteNullableBoolAsync(NpgsqlBinaryImporter writer, bool? value, CancellationToken ct)
    {
        if (value.HasValue)
            await writer.WriteAsync(value.Value, NpgsqlDbType.Boolean, ct);
        else
            await writer.WriteNullAsync(ct);
    }
}
