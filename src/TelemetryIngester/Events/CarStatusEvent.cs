namespace TelemetryIngester.Events;

/// <summary>
/// Combined car health and energy status, published to the <c>car-status</c> Kafka topic.
/// <para>
/// This event is produced by <b>two different</b> source packets — both emit <c>EventType = "CarStatus"</c>
/// so they share a single Kafka topic and TimescaleDB table:
/// <list type="bullet">
///   <item><description><b>CarDamageDataPacket</b> → populates tyre wear fields; all other fields are <c>null</c>.</description></item>
///   <item><description><b>CarStatusDataPacket</b> → populates fuel, ERS, and tyre compound fields; tyre wear fields are <c>null</c>.</description></item>
/// </list>
/// The TimescaleDB writer (next PR) handles these partial writes with nullable columns.
/// </para>
/// </summary>
public sealed record CarStatusEvent : TelemetryEvent
{
    // From CarDamageDataPacket — null when sourced from CarStatusDataPacket
    public required float? TyreWearRl { get; init; }
    public required float? TyreWearRr { get; init; }
    public required float? TyreWearFl { get; init; }
    public required float? TyreWearFr { get; init; }

    // From CarStatusDataPacket — null when sourced from CarDamageDataPacket

    /// <summary>
    /// The physics tyre compound fitted, as a numeric F1 UDP code
    /// (e.g. 16 = C5 soft, 17 = C4, 18 = C3, 19 = C2, 20 = C1 hard, 7 = intermediate, 8 = wet).
    /// </summary>
    public required int? ActualTyreCompound { get; init; }
    public required int? VisualTyreCompound { get; init; }
    public required int? TyresAgeLaps { get; init; }
    public required float? FuelInTank { get; init; }
    public required float? FuelCapacity { get; init; }
    public required float? FuelRemainingLaps { get; init; }

    /// <summary>Energy stored in the ERS (hybrid) battery in joules. Maximum is 4 MJ.</summary>
    public required float? ErsStoreEnergy { get; init; }

    /// <summary>ERS deployment mode: 0 = none, 1 = medium, 2 = hotlap, 3 = overtake.</summary>
    public required int? ErsDeployMode { get; init; }

    /// <summary>Energy harvested this lap via the MGU-K (Motor Generator Unit – Kinetic), in joules.</summary>
    public required float? ErsHarvestedLapMguk { get; init; }

    /// <summary>Energy harvested this lap via the MGU-H (Motor Generator Unit – Heat), in joules.</summary>
    public required float? ErsHarvestedLapMguh { get; init; }

    /// <summary>Total ERS energy deployed from the battery this lap, in joules.</summary>
    public required float? ErsDeployedLap { get; init; }
}
