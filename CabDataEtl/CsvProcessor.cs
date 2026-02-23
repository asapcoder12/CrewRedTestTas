using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;

namespace CabDataEtl
{
    /// <summary>
    /// CsvHelper class map – maps CSV column headers to <see cref="TaxiTripRecord"/> properties.
    /// Only the columns we care about are mapped; the rest are ignored automatically.
    /// </summary>
    public sealed class TaxiTripCsvMap : ClassMap<TaxiTripRecord>
    {
        public TaxiTripCsvMap()
        {
            Map(m => m.PickupDateTime).Name("tpep_pickup_datetime");
            Map(m => m.DropoffDateTime).Name("tpep_dropoff_datetime");
            Map(m => m.PassengerCount).Name("passenger_count");
            Map(m => m.TripDistance).Name("trip_distance");
            Map(m => m.StoreAndFwdFlag).Name("store_and_fwd_flag");
            Map(m => m.PULocationID).Name("PULocationID");
            Map(m => m.DOLocationID).Name("DOLocationID");
            Map(m => m.FareAmount).Name("fare_amount");
            Map(m => m.TipAmount).Name("tip_amount");
        }
    }

    /// <summary>
    /// Handles CSV reading, data cleaning, deduplication, and timezone conversion.
    /// </summary>
    public static class CsvProcessor
    {
        // Eastern Standard Time on Windows (handles EST/EDT transitions automatically)
        private static readonly TimeZoneInfo EstZone =
            TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

        /// <summary>
        /// Reads the CSV, cleans data, removes duplicates, converts EST→UTC.
        /// Returns the cleaned list and writes removed duplicates to <paramref name="duplicatesCsvPath"/>.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when a path argument is null or empty.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the CSV input file does not exist.</exception>
        /// <exception cref="IOException">Thrown when file I/O fails.</exception>
        public static List<TaxiTripRecord> ProcessCsv(string csvPath, string duplicatesCsvPath)
        {
            if (string.IsNullOrWhiteSpace(csvPath))
                throw new ArgumentException("CSV file path cannot be null or empty.", nameof(csvPath));
            if (string.IsNullOrWhiteSpace(duplicatesCsvPath))
                throw new ArgumentException("Duplicates CSV path cannot be null or empty.", nameof(duplicatesCsvPath));
            if (!File.Exists(csvPath))
                throw new FileNotFoundException($"CSV input file not found: '{csvPath}'.", csvPath);

            Console.WriteLine($"Reading CSV from: {csvPath}");

            var allRecords = ParseCsv(csvPath);
            allRecords = FilterIncompleteRecords(allRecords);
            CleanTextFields(allRecords);

            var (unique, duplicates) = DeduplicateRecords(allRecords);
            WriteDuplicatesCsv(duplicates, duplicatesCsvPath);
            ConvertEstToUtc(unique);

            return unique;
        }

        /// <summary>
        /// Parses the CSV file into a list of records, skipping bad data rows.
        /// </summary>
        private static List<TaxiTripRecord> ParseCsv(string csvPath)
        {
            int badRowCount = 0;
            List<TaxiTripRecord> records;

            try
            {
                using (var reader = new StreamReader(csvPath))
                using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    TrimOptions = TrimOptions.Trim,
                    MissingFieldFound = null,
                    BadDataFound = context =>
                    {
                        badRowCount++;
                    }
                }))
                {
                    csv.Context.RegisterClassMap<TaxiTripCsvMap>();
                    records = csv.GetRecords<TaxiTripRecord>().ToList();
                }
            }
            catch (HeaderValidationException ex)
            {
                throw new FormatException(
                    "CSV header validation failed. Ensure the CSV file has the expected column headers.",
                    ex);
            }
            catch (CsvHelperException ex)
            {
                throw new FormatException(
                    $"Failed to parse CSV file '{csvPath}'. The file may be malformed or use an unexpected format.",
                    ex);
            }

            Console.WriteLine($"  Parsed {records.Count:N0} records from CSV ({badRowCount} bad data events skipped).");
            return records;
        }

        /// <summary>
        /// Removes records that have any missing required fields.
        /// </summary>
        private static List<TaxiTripRecord> FilterIncompleteRecords(List<TaxiTripRecord> records)
        {
            int beforeFilter = records.Count;
            var filtered = records
                .Where(r => r.PickupDateTime.HasValue
                         && r.DropoffDateTime.HasValue
                         && r.PassengerCount.HasValue
                         && r.TripDistance.HasValue
                         && r.PULocationID.HasValue
                         && r.DOLocationID.HasValue
                         && r.FareAmount.HasValue
                         && r.TipAmount.HasValue)
                .ToList();

            int droppedNulls = beforeFilter - filtered.Count;
            if (droppedNulls > 0)
                Console.WriteLine($"  Dropped {droppedNulls:N0} records with missing required fields.");

            return filtered;
        }

        /// <summary>
        /// Trims whitespace and converts store_and_fwd_flag values (N→No, Y→Yes).
        /// </summary>
        private static void CleanTextFields(List<TaxiTripRecord> records)
        {
            foreach (var r in records)
            {
                r.StoreAndFwdFlag = (r.StoreAndFwdFlag ?? string.Empty).Trim();

                r.StoreAndFwdFlag = r.StoreAndFwdFlag.ToUpperInvariant() switch
                {
                    "N" => "No",
                    "Y" => "Yes",
                    _ => r.StoreAndFwdFlag
                };
            }
        }

        /// <summary>
        /// Splits records into unique and duplicate lists based on (pickup, dropoff, passenger_count).
        /// </summary>
        private static (List<TaxiTripRecord> unique, List<TaxiTripRecord> duplicates) DeduplicateRecords(
            List<TaxiTripRecord> records)
        {
            var seen = new HashSet<(DateTime, DateTime, int)>();
            var unique = new List<TaxiTripRecord>(records.Count);
            var duplicates = new List<TaxiTripRecord>();

            foreach (var r in records)
            {
                var key = (r.PickupDateTime!.Value, r.DropoffDateTime!.Value, r.PassengerCount!.Value);
                if (!seen.Add(key))
                {
                    duplicates.Add(r);
                }
                else
                {
                    unique.Add(r);
                }
            }

            Console.WriteLine($"  Removed {duplicates.Count:N0} duplicates. Unique records: {unique.Count:N0}.");
            return (unique, duplicates);
        }

        /// <summary>
        /// Writes duplicate records to a CSV file.
        /// </summary>
        private static void WriteDuplicatesCsv(List<TaxiTripRecord> duplicates, string duplicatesCsvPath)
        {
            try
            {
                using (var writer = new StreamWriter(duplicatesCsvPath))
                using (var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csvWriter.Context.RegisterClassMap<TaxiTripCsvMap>();
                    csvWriter.WriteRecords(duplicates);
                }
            }
            catch (IOException ex)
            {
                throw new IOException(
                    $"Failed to write duplicates CSV to '{duplicatesCsvPath}'. Ensure the directory exists and is writable.",
                    ex);
            }

            Console.WriteLine($"  Duplicates written to: {duplicatesCsvPath}");
        }

        /// <summary>
        /// Converts pickup and dropoff datetimes from EST to UTC.
        /// </summary>
        private static void ConvertEstToUtc(List<TaxiTripRecord> records)
        {
            foreach (var r in records)
            {
                r.PickupDateTime = TimeZoneInfo.ConvertTimeToUtc(
                    DateTime.SpecifyKind(r.PickupDateTime!.Value, DateTimeKind.Unspecified), EstZone);
                r.DropoffDateTime = TimeZoneInfo.ConvertTimeToUtc(
                    DateTime.SpecifyKind(r.DropoffDateTime!.Value, DateTimeKind.Unspecified), EstZone);
            }

            Console.WriteLine("  Converted datetimes from EST to UTC.");
        }
    }
}
