using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;
using TelemetryIngester.Kafka;
using TelemetryIngester.Services;
using TelemetryIngester.Storage;

namespace TelemetryIngester.Tests.Integration;

public sealed class KafkaToTimescaleIntegrationTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.8.0").Build();

    private readonly PostgreSqlContainer _pgContainer = new PostgreSqlBuilder("timescale/timescaledb:latest-pg16")
        .Build();

    private string _bootstrapServers = null!;
    private string _connectionString = null!;

    public async Task InitializeAsync()
    {
        // Start both containers in parallel.
        await Task.WhenAll(
            _kafkaContainer.StartAsync(),
            _pgContainer.StartAsync());

        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();
        _connectionString = _pgContainer.GetConnectionString();

        // Initialize TimescaleDB schema.
        var initSql = await File.ReadAllTextAsync(
            Path.Combine(FindRepoRoot(), "infra", "timescaledb", "init.sql"));

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(initSql, conn);
        await cmd.ExecuteNonQueryAsync();

        // Create Kafka topics.
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();

        await adminClient.CreateTopicsAsync(
        [
            new TopicSpecification { Name = "car-telemetry", NumPartitions = 1, ReplicationFactor = 1 },
            new TopicSpecification { Name = "lap-data", NumPartitions = 1, ReplicationFactor = 1 },
            new TopicSpecification { Name = "car-status", NumPartitions = 1, ReplicationFactor = 1 },
        ]);
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(
            _kafkaContainer.DisposeAsync().AsTask(),
            _pgContainer.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task ProduceAndConsume_CarTelemetryEvents_WrittenToTimescaleDb()
    {
        await using var producer = CreateProducer();
        var writer = CreateWriter();
        var consumer = CreateConsumerService(writer, batchSize: 5, flushIntervalMs: 200);

        var events = new[]
        {
            MakeCarTelemetryEvent(frameId: 1),
            MakeCarTelemetryEvent(frameId: 2),
            MakeCarTelemetryEvent(frameId: 3),
        };

        foreach (var e in events)
            await producer.ProduceAsync(e);

        await consumer.StartAsync(CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await WaitForRowsAsync("car_telemetry", 3, cts.Token);

        await consumer.StopAsync(CancellationToken.None);

        // Verify field values for one row.
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT speed, gear, drs FROM car_telemetry WHERE frame_id = 1", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal((short)300, reader.GetInt16(0));
        Assert.Equal((short)7, reader.GetInt16(1));
        Assert.True(reader.GetBoolean(2));
    }

    [Fact]
    public async Task ProduceAndConsume_MixedEventTypes_WrittenToCorrectTables()
    {
        await using var producer = CreateProducer();
        var writer = CreateWriter();
        var consumer = CreateConsumerService(writer, batchSize: 5, flushIntervalMs: 200);

        await producer.ProduceAsync(MakeCarTelemetryEvent(frameId: 10));
        await producer.ProduceAsync(MakeCarTelemetryEvent(frameId: 11));
        await producer.ProduceAsync(MakeLapDataEvent(frameId: 12));
        await producer.ProduceAsync(MakeCarStatusEvent(frameId: 13, fromDamagePacket: false));
        await producer.ProduceAsync(MakeCarStatusEvent(frameId: 14, fromDamagePacket: true));

        await consumer.StartAsync(CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        // Wait for all tables to have expected rows.
        await Task.WhenAll(
            WaitForRowsAsync("car_telemetry", 2, cts.Token),
            WaitForRowsAsync("lap_data", 1, cts.Token),
            WaitForRowsAsync("car_status", 2, cts.Token));

        await consumer.StopAsync(CancellationToken.None);

        // Spot-check: verify the lap data row has correct field values.
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT current_lap_num, car_position FROM lap_data WHERE frame_id = 12", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal((short)5, reader.GetInt16(0));
        Assert.Equal((short)3, reader.GetInt16(1));
    }

    [Fact]
    public async Task ProduceAndConsume_NullableFields_PreservedThroughPipeline()
    {
        await using var producer = CreateProducer();
        var writer = CreateWriter();
        var consumer = CreateConsumerService(writer, batchSize: 5, flushIntervalMs: 200);

        // From damage packet: tyre wear set, fuel/ERS null.
        await producer.ProduceAsync(MakeCarStatusEvent(frameId: 30, fromDamagePacket: true));

        await consumer.StartAsync(CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await WaitForRowsAsync("car_status", 1, cts.Token);

        await consumer.StopAsync(CancellationToken.None);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT tyre_wear_rl, fuel_in_tank, ers_deploy_mode FROM car_status WHERE frame_id = 30", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0.15f, reader.GetFloat(0), precision: 5);
        Assert.True(reader.IsDBNull(1)); // fuel null when from damage packet
        Assert.True(reader.IsDBNull(2)); // ERS null when from damage packet
    }

    [Fact]
    public async Task ProduceAndConsume_PartialBatch_FlushedByTimer()
    {
        await using var producer = CreateProducer();
        var writer = CreateWriter();
        // Large batch size so we rely on the time-based flush.
        var consumer = CreateConsumerService(writer, batchSize: 1000, flushIntervalMs: 200);

        await producer.ProduceAsync(MakeCarTelemetryEvent(frameId: 50));

        await consumer.StartAsync(CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await WaitForRowsAsync("car_telemetry", 1, cts.Token);

        await consumer.StopAsync(CancellationToken.None);
    }

    // -- Service factories --

    private KafkaProducer CreateProducer()
    {
        var options = Options.Create(new KafkaOptions { BootstrapServers = _bootstrapServers });
        return new KafkaProducer(options, NullLogger<KafkaProducer>.Instance);
    }

    private TimescaleWriter CreateWriter()
    {
        var options = Options.Create(new TimescaleDbOptions { ConnectionString = _connectionString });
        return new TimescaleWriter(options, NullLogger<TimescaleWriter>.Instance);
    }

    private KafkaConsumerService CreateConsumerService(
        TimescaleWriter writer, int batchSize, int flushIntervalMs)
    {
        var kafkaOpts = Options.Create(new KafkaOptions
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-{Guid.NewGuid()}",
        });
        var ingesterOpts = Options.Create(new IngesterOptions
        {
            BatchSize = batchSize,
            FlushIntervalMs = flushIntervalMs,
        });

        return new KafkaConsumerService(
            kafkaOpts,
            ingesterOpts,
            writer,
            NullLogger<KafkaConsumerService>.Instance);
    }

    // -- Helpers --

    private async Task WaitForRowsAsync(string table, long expectedCount, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {table}", conn);
            var count = (long)(await cmd.ExecuteScalarAsync(ct))!;
            if (count >= expectedCount)
                return;

            await Task.Delay(100, ct);
        }

        ct.ThrowIfCancellationRequested();
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
