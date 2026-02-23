using System;
using System.Diagnostics;
using System.IO;
using CabDataEtl;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace CabDataEtl
{
    public class Program
    {
        public static int Main(string[] args)
        {
            IConfiguration configuration;
            try
            {
                configuration = new ConfigurationBuilder()
                    .SetBasePath(AppContext.BaseDirectory)
                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                    .Build();
            }
            catch (FileNotFoundException ex)
            {
                Console.Error.WriteLine($"ERROR: Configuration file 'appsettings.json' not found at '{AppContext.BaseDirectory}'.");
                Console.Error.WriteLine(ex.Message);
                return 1;
            }

            string? masterConnectionString = configuration.GetConnectionString("MasterConnection");
            string? dbConnectionString = configuration.GetConnectionString("TaxiTripsDb");

            if (string.IsNullOrWhiteSpace(masterConnectionString))
            {
                Console.Error.WriteLine("ERROR: Missing 'MasterConnection' in appsettings.json ConnectionStrings section.");
                return 1;
            }

            if (string.IsNullOrWhiteSpace(dbConnectionString))
            {
                Console.Error.WriteLine("ERROR: Missing 'TaxiTripsDb' in appsettings.json ConnectionStrings section.");
                return 1;
            }

            // Resolve paths
            string projectDir = AppContext.BaseDirectory;

            // Walk up from bin/Debug/net8.0 → CabDataEtl (project dir) → CrewRedTestTask (solution root)
            string solutionRoot = Path.GetFullPath(Path.Combine(projectDir, "..", "..", ".."));
            solutionRoot = Path.GetFullPath(Path.Combine(solutionRoot, ".."));

            string csvPath = args.Length > 0
                ? args[0]
                : Path.Combine(solutionRoot, "docs", "sample-cab-data.csv");

            string sqlScriptsFolder = Path.Combine(solutionRoot, "sql");
            string outputFolder = Path.Combine(solutionRoot, "output");
            Directory.CreateDirectory(outputFolder);
            string duplicatesCsvPath = Path.Combine(outputFolder, "duplicates.csv");

            if (!File.Exists(csvPath))
            {
                Console.Error.WriteLine($"ERROR: CSV file not found at '{csvPath}'.");
                Console.Error.WriteLine("Usage: dotnet run [<csv-path>]");
                return 1;
            }

            // Run ETL (extract/transform/load) pipeline
            Stopwatch stopwatch = Stopwatch.StartNew();
            var dbService = new DatabaseService(masterConnectionString, dbConnectionString);

            try
            {
                // Step 1 – Set up database & table
                dbService.EnsureDatabaseAndTable(sqlScriptsFolder);

                // Step 2 – Parse, clean, deduplicate, convert timezones
                var records = CsvProcessor.ProcessCsv(csvPath, duplicatesCsvPath);

                // Step 3 – Bulk insert into SQL Server
                dbService.BulkInsert(records);

                // Step 4 – Report results
                int rowCount = dbService.GetRowCount();
                stopwatch.Stop();

                Console.WriteLine();
                Console.WriteLine("═══════════════════════════════════════════");
                Console.WriteLine($"  ETL complete in {stopwatch.Elapsed.TotalSeconds:F1}s");
                Console.WriteLine($"  Rows in TaxiTrips table: {rowCount:N0}");
                Console.WriteLine("═══════════════════════════════════════════");
            }
            catch (InvalidOperationException ex) when (ex.InnerException is SqlException)
            {
                Console.Error.WriteLine($"DATABASE ERROR: {ex.Message}");
                Console.Error.WriteLine($"  SQL Details: {ex.InnerException.Message}");
                return 1;
            }
            catch (FormatException ex)
            {
                Console.Error.WriteLine($"CSV FORMAT ERROR: {ex.Message}");
                if (ex.InnerException != null)
                    Console.Error.WriteLine($"  Details: {ex.InnerException.Message}");
                return 1;
            }
            catch (FileNotFoundException ex)
            {
                Console.Error.WriteLine($"FILE NOT FOUND: {ex.Message}");
                return 1;
            }
            catch (DirectoryNotFoundException ex)
            {
                Console.Error.WriteLine($"DIRECTORY NOT FOUND: {ex.Message}");
                return 1;
            }
            catch (IOException ex)
            {
                Console.Error.WriteLine($"I/O ERROR: {ex.Message}");
                return 1;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"UNEXPECTED ERROR: {ex.Message}");
                Console.Error.WriteLine(ex.StackTrace);
                return 1;
            }

            return 0;
        }
    }
}
