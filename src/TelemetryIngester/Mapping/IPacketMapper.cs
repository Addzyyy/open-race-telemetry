using F1Game.UDP.Packets;
using TelemetryIngester.Events;

namespace TelemetryIngester.Mapping;

public interface IPacketMapper
{
    IReadOnlyList<TelemetryEvent> MapPacket(UnionPacket packet);
}
