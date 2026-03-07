namespace TelemetryIngester.Events;

public sealed record CarStatusEvent : TelemetryEvent
{
    // From CarDamageDataPacket
    public required float? TyreWearRl { get; init; }
    public required float? TyreWearRr { get; init; }
    public required float? TyreWearFl { get; init; }
    public required float? TyreWearFr { get; init; }

    // From CarStatusDataPacket
    public required int? ActualTyreCompound { get; init; }
    public required int? VisualTyreCompound { get; init; }
    public required int? TyresAgeLaps { get; init; }
    public required float? FuelInTank { get; init; }
    public required float? FuelCapacity { get; init; }
    public required float? FuelRemainingLaps { get; init; }
    public required float? ErsStoreEnergy { get; init; }
    public required int? ErsDeployMode { get; init; }
    public required float? ErsHarvestedLapMguk { get; init; }
    public required float? ErsHarvestedLapMguh { get; init; }
    public required float? ErsDeployedLap { get; init; }
}
