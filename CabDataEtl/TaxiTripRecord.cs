namespace CabDataEtl;

/// <summary>
/// Represents a single taxi trip record with only the columns we need to store.
/// Nullable types are used to gracefully handle empty/missing CSV values.
/// </summary>
public class TaxiTripRecord
{
    public DateTime? PickupDateTime { get; set; }
    public DateTime? DropoffDateTime { get; set; }
    public int? PassengerCount { get; set; }
    public decimal? TripDistance { get; set; }
    public string? StoreAndFwdFlag { get; set; }
    public int? PULocationID { get; set; }
    public int? DOLocationID { get; set; }
    public decimal? FareAmount { get; set; }
    public decimal? TipAmount { get; set; }
}
