using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;
using TelemetryIngester.Storage;

namespace TelemetryIngester.Tests.Storage;

public sealed class TimescaleWriterIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("timescale/timescaledb:latest-pg16")
        .Build();

    private TimescaleWriter _writer = null!;
    private string _connectionString = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();

        // Run init.sql to create tables and hypertables.
        var initSql = await File.ReadAllTextAsync(
            Path.Combine(FindRepoRoot(), "infra", "timescaledb", "init.sql"));

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(initSql, conn);
        await cmd.ExecuteNonQueryAsync();

        var options = Options.Create(new TimescaleDbOptions { ConnectionString = _connectionString });
        _writer = new TimescaleWriter(options, NullLogger<TimescaleWriter>.Instance);
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    [Fact]
    public async Task WriteBatch_CarTelemetryEvents_InsertsRows()
    {
        var events = new List<TelemetryEvent>
        {
            MakeCarTelemetryEvent(frameId: 1),
            MakeCarTelemetryEvent(frameId: 2),
        };

        await _writer.WriteBatchAsync(events, CancellationToken.None);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM car_telemetry", conn);
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        Assert.Equal(2, count);

        await using var cmd2 = new NpgsqlCommand("SELECT speed, gear, drs FROM car_telemetry WHERE frame_id = 1", conn);
        await using var reader = await cmd2.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal((short)300, reader.GetInt16(0));
        Assert.Equal((short)7, reader.GetInt16(1));
        Assert.True(reader.GetBoolean(2));
    }

    [Fact]
    public async Task WriteBatch_LapDataEvents_InsertsRows()
    {
        var events = new List<TelemetryEvent>
        {
            MakeLapDataEvent(frameId: 10),
        };

        await _writer.WriteBatchAsync(events, CancellationToken.None);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT current_lap_num, sector1_time_ms, sector3_time_ms FROM lap_data WHERE frame_id = 10", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal((short)5, reader.GetInt16(0));
        Assert.Equal(28000, reader.GetInt32(1));
        Assert.True(reader.IsDBNull(2)); // sector3 is always null from this source
    }

    [Fact]
    public async Task WriteBatch_CarStatusEvents_WithNulls_InsertsRows()
    {
        var events = new List<TelemetryEvent>
        {
            MakeCarStatusEvent(frameId: 20, fromDamagePacket: true),
        };

        await _writer.WriteBatchAsync(events, CancellationToken.None);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT tyre_wear_rl, fuel_in_tank, ers_deploy_mode FROM car_status WHERE frame_id = 20", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0.15f, reader.GetFloat(0), precision: 5);
        Assert.True(reader.IsDBNull(1)); // fuel fields null when from damage packet
        Assert.True(reader.IsDBNull(2));
    }

    [Fact]
    public async Task WriteBatch_MixedEventTypes_WritesToAllTables()
    {
        var events = new List<TelemetryEvent>
        {
            MakeCarTelemetryEvent(frameId: 100),
            MakeLapDataEvent(frameId: 101),
            MakeCarStatusEvent(frameId: 102, fromDamagePacket: false),
        };

        await _writer.WriteBatchAsync(events, CancellationToken.None);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        Assert.Equal(1L, await CountRowsAsync(conn, "car_telemetry"));
        Assert.Equal(1L, await CountRowsAsync(conn, "lap_data"));
        Assert.Equal(1L, await CountRowsAsync(conn, "car_status"));
    }

    [Fact]
    public async Task WriteBatch_EmptyBatch_DoesNotThrow()
    {
        await _writer.WriteBatchAsync([], CancellationToken.None);
    }

    private static async Task<long> CountRowsAsync(NpgsqlConnection conn, string table)
    {
        await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {table}", conn);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static CarTelemetryEvent MakeCarTelemetryEvent(uint frameId) => new()
    {
        EventType = "CarTelemetry",
        SessionUid = "99999",
        Timestamp = DateTimeOffset.UtcNow,
        FrameId = frameId,
        CarIndex = 0,
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

    private static LapDataEvent MakeLapDataEvent(uint frameId) => new()
    {
        EventType = "LapData",
        SessionUid = "99999",
        Timestamp = DateTimeOffset.UtcNow,
        FrameId = frameId,
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

    private static CarStatusEvent MakeCarStatusEvent(uint frameId, bool fromDamagePacket) => new()
    {
        EventType = "CarStatus",
        SessionUid = "99999",
        Timestamp = DateTimeOffset.UtcNow,
        FrameId = frameId,
        CarIndex = 0,
        TyreWearRl = fromDamagePacket ? 0.15f : null,
        TyreWearRr = fromDamagePacket ? 0.20f : null,
        TyreWearFl = fromDamagePacket ? 0.10f : null,
        TyreWearFr = fromDamagePacket ? 0.18f : null,
        ActualTyreCompound = fromDamagePacket ? null : 16,
        VisualTyreCompound = fromDamagePacket ? null : 16,
        TyresAgeLaps = fromDamagePacket ? null : 8,
        FuelInTank = fromDamagePacket ? null : 50.0f,
        FuelCapacity = fromDamagePacket ? null : 110.0f,
        FuelRemainingLaps = fromDamagePacket ? null : 12.5f,
        ErsStoreEnergy = fromDamagePacket ? null : 3_500_000f,
        ErsDeployMode = fromDamagePacket ? null : 2,
        ErsHarvestedLapMguk = fromDamagePacket ? null : 500_000f,
        ErsHarvestedLapMguh = fromDamagePacket ? null : 1_000_000f,
        ErsDeployedLap = fromDamagePacket ? null : 800_000f,
    };

    private static string FindRepoRoot()
    {
        var dir = Directory.GetCurrentDirectory();
        while (dir is not null)
        {
            if (Directory.Exists(Path.Combine(dir, ".git")))
                return dir;
            dir = Directory.GetParent(dir)?.FullName;
        }
        throw new InvalidOperationException("Could not find repository root");
    }
}
