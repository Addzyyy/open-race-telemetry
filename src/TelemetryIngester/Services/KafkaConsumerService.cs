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
    private static readonly string[] Topics = ["car-telemetry", "lap-data", "car-status"];

    private readonly KafkaOptions _kafkaOptions = kafkaOptions.Value;
    private readonly IngesterOptions _ingesterOptions = ingesterOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(Topics);

        logger.LogInformation("Kafka consumer started, subscribed to {Topics}", string.Join(", ", Topics));

        var buffer = new List<TelemetryEvent>();
        var offsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
        var flushStopwatch = Stopwatch.StartNew();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // Confluent's Consume is synchronous — run on the thread pool to avoid blocking.
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

                        // Track highest offset per topic-partition for batch commit.
                        offsets[consumeResult.TopicPartition] = consumeResult.TopicPartitionOffset;
                    }
                    catch (JsonException ex)
                    {
                        logger.LogWarning(ex, "Failed to deserialize message from {Topic}, skipping",
                            consumeResult.Topic);
                    }
                }

                // Dual-trigger flush: size threshold OR time threshold.
                var shouldFlush = buffer.Count >= _ingesterOptions.BatchSize
                    || (buffer.Count > 0 && flushStopwatch.ElapsedMilliseconds >= _ingesterOptions.FlushIntervalMs);

                if (shouldFlush)
                {
                    try
                    {
                        await writer.WriteBatchAsync(buffer, stoppingToken);

                        // Commit highest offset per topic-partition in a single call.
                        consumer.Commit(offsets.Values);

                        logger.LogDebug("Flushed {Count} events to TimescaleDB", buffer.Count);
                        buffer.Clear();
                        offsets.Clear();
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        // Retain buffer — no commit, retry next cycle.
                        logger.LogError(ex, "Failed to write batch of {Count} events, will retry", buffer.Count);
                    }

                    flushStopwatch.Restart();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }

        // Flush remaining buffer on shutdown.
        if (buffer.Count > 0)
        {
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

        consumer.Close();
        logger.LogInformation("Kafka consumer stopped");
    }
}
