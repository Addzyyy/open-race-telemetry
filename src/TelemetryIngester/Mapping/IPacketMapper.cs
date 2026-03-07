using F1Game.UDP.Packets;
using TelemetryIngester.Events;

namespace TelemetryIngester.Mapping;

/// <summary>
/// Translates a raw F1 25 UDP packet into zero or more canonical <see cref="TelemetryEvent"/> records.
/// <para>
/// The game broadcasts many different packet types (~60 Hz per type). This mapper inspects
/// each incoming packet, discards types we don't care about, and converts the ones we do
/// (car telemetry, lap data, car status, car damage) into strongly-typed event records that
/// the rest of the pipeline can work with without knowing anything about the F1Game.UDP library.
/// </para>
/// </summary>
public interface IPacketMapper
{
    /// <summary>
    /// Maps a single UDP packet to a list of telemetry events.
    /// </summary>
    /// <param name="packet">
    /// A <see cref="UnionPacket"/> — a discriminated union that wraps whichever specific
    /// packet type the game sent (car telemetry, lap data, session info, etc.).
    /// </param>
    /// <returns>
    /// One event per car that was mapped. Returns an empty list for packet types that
    /// are not handled (motion, session, participants, etc.).
    /// </returns>
    IReadOnlyList<TelemetryEvent> MapPacket(UnionPacket packet);
}
