using System.Net.Sockets;
using F1Game.UDP;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TelemetryIngester.Configuration;
using TelemetryIngester.Kafka;
using TelemetryIngester.Mapping;

namespace TelemetryIngester.Services;

public sealed class UdpListenerService(
    IOptions<TelemetryOptions> options,
    IPacketMapper mapper,
    IKafkaProducer producer,
    ILogger<UdpListenerService> logger) : BackgroundService
{
    private readonly TelemetryOptions _options = options.Value;
    private ulong _currentSessionUid;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var udpClient = new UdpClient(_options.ListenPort);
        logger.LogInformation("UDP listener started on port {Port}", _options.ListenPort);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(stoppingToken);
                var packet = result.Buffer.ToPacket();

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
                break;
            }
            catch (SocketException ex)
            {
                logger.LogWarning(ex, "Socket error receiving UDP packet");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error processing UDP packet");
            }
        }

        logger.LogInformation("UDP listener stopped");
    }
}
