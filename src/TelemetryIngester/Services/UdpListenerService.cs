using System.Net.Sockets;
using F1Game.UDP;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Kafka;
using TelemetryIngester.Mapping;

namespace TelemetryIngester.Services;

/// <summary>
/// Listens for F1 25 telemetry UDP packets, decodes them, and publishes the resulting
/// events to Kafka. This is the entry point for all data in the pipeline.
/// <para>
/// The F1 game broadcasts UDP datagrams at ~60 Hz on a configurable port (default 20777).
/// Each datagram contains one packet type (car telemetry, lap data, session info, etc.).
/// </para>
/// </summary>
public sealed class UdpListenerService(
    IOptions<TelemetryOptions> options,
    IPacketMapper mapper,
    IKafkaProducer producer,
    ILogger<UdpListenerService> logger) : BackgroundService
{
    private readonly TelemetryOptions _options = options.Value;

    // Tracks the current session UID so we can log when a new game session starts.
    private ulong _currentSessionUid;

    /// <summary>
    /// Main receive loop. Runs for the lifetime of the application and is cancelled
    /// automatically when the host receives a shutdown signal (e.g. Ctrl+C).
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var udpClient = new UdpClient(_options.ListenPort);
        logger.LogInformation("UDP listener started on port {Port}", _options.ListenPort);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(stoppingToken);

                // ToPacket() is an F1Game.UDP extension method that reads the raw bytes
                // and returns a UnionPacket — a discriminated union of all possible packet types.
                var packet = result.Buffer.ToPacket();

                // Detect when the player starts a new session (new race, qualifying, etc.)
                // SessionUID is 0 between sessions, so we skip the zero value.
                var sessionUid = packet.Header.SessionUID;
                if (sessionUid != _currentSessionUid && sessionUid != 0)
                {
                    _currentSessionUid = sessionUid;
                    logger.LogInformation("Session detected: {SessionUid}", sessionUid);
                }

                var events = mapper.MapPacket(packet);
                logger.LogDebug("Received {PacketType}, produced {EventCount} event(s)", packet.PacketType, events.Count);

                foreach (var @event in events)
                    await producer.ProduceAsync(@event, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown — the host cancelled the token. Exit the loop cleanly.
                break;
            }
            catch (SocketException ex)
            {
                // Transient network error. Log and continue rather than crashing.
                logger.LogWarning(ex, "Socket error receiving UDP packet");
            }
            catch (Exception ex)
            {
                // Unexpected error processing a single packet. Log and continue so a
                // malformed packet doesn't take down the whole ingestion loop.
                logger.LogError(ex, "Unexpected error processing UDP packet");
            }
        }

        logger.LogInformation("UDP listener stopped");
    }
}
