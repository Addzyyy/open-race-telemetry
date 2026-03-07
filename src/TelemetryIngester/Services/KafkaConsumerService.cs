using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Events;
using TelemetryIngester.Kafka;
using TelemetryIngester.Storage;

namespace TelemetryIngester.Services;

public sealed class KafkaConsumerService(
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<IngesterOptions> ingesterOptions,
    ITimescaleWriter writer,
    ILogger<KafkaConsumerService> logger) : BackgroundService
{
    private static readonly string[] Topics = ["car-telemetry", "lap-data", "car-status", "participants", "session", "session-history"];

    private static readonly TimeSpan InitialBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly KafkaOptions _kafkaOptions = kafkaOptions.Value;
    private readonly IngesterOptions _ingesterOptions = ingesterOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var buffer = new List<TelemetryEvent>();
        var offsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
        var connectBackoff = InitialBackoff;

        while (!stoppingToken.IsCancellationRequested)
        {
            IConsumer<string, string>? consumer = null;
            try
            {
                consumer = BuildConsumer();
                consumer.Subscribe(Topics);
                logger.LogInformation("Kafka consumer connected, subscribed to {Topics}", string.Join(", ", Topics));
                connectBackoff = InitialBackoff;

                await ConsumeLoop(consumer, buffer, offsets, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown — flush remaining buffer then exit.
                if (consumer is not null)
                    await FlushRemainingBuffer(consumer, buffer, offsets);
                break;
            }
            catch (Exception ex)
            {
                logger.LogInformation(ex, "Kafka consumer disconnected, reconnecting in {Delay}s", connectBackoff.TotalSeconds);
            }
            finally
            {
                consumer?.Close();
                consumer?.Dispose();
            }

            // Backoff before reconnecting.
            await Task.Delay(connectBackoff, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            connectBackoff = NextBackoff(connectBackoff);
        }

        logger.LogInformation("Kafka consumer stopped");
    }

    private IConsumer<string, string> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        return new ConsumerBuilder<string, string>(config).Build();
    }

    private async Task ConsumeLoop(
        IConsumer<string, string> consumer,
        List<TelemetryEvent> buffer,
        Dictionary<TopicPartition, TopicPartitionOffset> offsets,
        CancellationToken stoppingToken)
    {
        var flushStopwatch = Stopwatch.StartNew();
        var writeBackoff = InitialBackoff;

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = await Task.Run(() =>
            {
                try
                {
                    return consumer.Consume(TimeSpan.FromMilliseconds(100));
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
            }, stoppingToken);

            if (consumeResult?.Message?.Value is not null)
            {
                try
                {
                    var @event = KafkaMessageSerializer.Deserialize(consumeResult.Message.Value);
                    buffer.Add(@event);
                    offsets[consumeResult.TopicPartition] = consumeResult.TopicPartitionOffset;
                }
                catch (JsonException ex)
                {
                    logger.LogWarning(ex, "Failed to deserialize message from {Topic}, skipping",
                        consumeResult.Topic);
                }
            }

            var shouldFlush = buffer.Count >= _ingesterOptions.BatchSize
                || (buffer.Count > 0 && flushStopwatch.ElapsedMilliseconds >= _ingesterOptions.FlushIntervalMs);

            if (shouldFlush)
            {
                try
                {
                    await writer.WriteBatchAsync(buffer, stoppingToken);
                    consumer.Commit(offsets.Values);
                    logger.LogDebug("Flushed {Count} events to TimescaleDB", buffer.Count);
                    buffer.Clear();
                    offsets.Clear();
                    writeBackoff = InitialBackoff;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogError(ex, "Failed to write batch of {Count} events, retrying in {Delay}s",
                        buffer.Count, writeBackoff.TotalSeconds);
                    await Task.Delay(writeBackoff, stoppingToken);
                    writeBackoff = NextBackoff(writeBackoff);
                }

                flushStopwatch.Restart();
            }
        }
    }

    private async Task FlushRemainingBuffer(
        IConsumer<string, string> consumer,
        List<TelemetryEvent> buffer,
        Dictionary<TopicPartition, TopicPartitionOffset> offsets)
    {
        if (buffer.Count == 0)
            return;

        try
        {
            await writer.WriteBatchAsync(buffer, CancellationToken.None);
            consumer.Commit(offsets.Values);
            logger.LogInformation("Flushed final {Count} events on shutdown", buffer.Count);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to flush final {Count} events on shutdown", buffer.Count);
        }
    }

    private static TimeSpan NextBackoff(TimeSpan current) =>
        current >= MaxBackoff ? MaxBackoff : TimeSpan.FromTicks(Math.Min(current.Ticks * 2, MaxBackoff.Ticks));
}
