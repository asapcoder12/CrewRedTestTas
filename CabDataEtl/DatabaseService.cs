using System;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace CabDataEtl
{
    /// <summary>
    /// Manages database creation, table setup, and bulk insertion into SQL Server LocalDB.
    /// Connection strings are injected via constructor — never hardcoded.
    /// </summary>
    public class DatabaseService
    {
        private readonly string _masterConnectionString;
        private readonly string _dbConnectionString;

        private const int DefaultBatchSize = 10_000;

        public DatabaseService(string masterConnectionString, string dbConnectionString)
        {
            _masterConnectionString = masterConnectionString
                ?? throw new ArgumentNullException(nameof(masterConnectionString));
            _dbConnectionString = dbConnectionString
                ?? throw new ArgumentNullException(nameof(dbConnectionString));
        }

        /// <summary>
        /// Ensures the database and table exist by running the SQL scripts.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when <paramref name="sqlScriptsFolder"/> is null or empty.</exception>
        /// <exception cref="FileNotFoundException">Thrown when a required SQL script file is not found.</exception>
        /// <exception cref="SqlException">Thrown when a SQL command fails.</exception>
        public void EnsureDatabaseAndTable(string sqlScriptsFolder)
        {
            if (string.IsNullOrWhiteSpace(sqlScriptsFolder))
                throw new ArgumentException("SQL scripts folder path cannot be null or empty.", nameof(sqlScriptsFolder));

            if (!Directory.Exists(sqlScriptsFolder))
                throw new DirectoryNotFoundException($"SQL scripts folder not found: '{sqlScriptsFolder}'.");

            Console.WriteLine("Setting up database...");

            // Create database (run against master)
            string createDbPath = Path.Combine(sqlScriptsFolder, "CreateDatabase.sql");
            if (!File.Exists(createDbPath))
                throw new FileNotFoundException("Database creation script not found.", createDbPath);

            string createDbSql = File.ReadAllText(createDbPath);
            try
            {
                ExecuteSql(_masterConnectionString, createDbSql);
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException(
                    $"Failed to create database. Verify that LocalDB is running and the connection string is correct. SQL Error: {ex.Number}",
                    ex);
            }

            Console.WriteLine("  Database 'TaxiTripsDb' ensured.");

            // Create table & indexes (run against TaxiTripsDb)
            string createTablePath = Path.Combine(sqlScriptsFolder, "CreateTable.sql");
            if (!File.Exists(createTablePath))
                throw new FileNotFoundException("Table creation script not found.", createTablePath);

            string createTableSql = File.ReadAllText(createTablePath);
            try
            {
                ExecuteSql(_dbConnectionString, createTableSql);
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException(
                    $"Failed to create table/indexes in TaxiTripsDb. SQL Error: {ex.Number}",
                    ex);
            }

            Console.WriteLine("  Table 'TaxiTrips' and indexes ensured.");
        }

        /// <summary>
        /// Bulk-inserts records into the TaxiTrips table using SqlBulkCopy.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="records"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="batchSize"/> is not positive.</exception>
        /// <exception cref="InvalidOperationException">Thrown when a database operation fails.</exception>
        public void BulkInsert(List<TaxiTripRecord> records, int batchSize = DefaultBatchSize)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));
            if (batchSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be a positive integer.");

            Console.WriteLine($"Bulk-inserting {records.Count:N0} records (batch size {batchSize:N0})...");

            // Build a DataTable matching the DB schema
            DataTable table = BuildDataTable(records);

            try
            {
                using var connection = new SqlConnection(_dbConnectionString);
                connection.Open();

                // Truncate table before fresh load (idempotent ETL)
                using (var cmd = new SqlCommand("TRUNCATE TABLE [dbo].[TaxiTrips]", connection))
                {
                    cmd.ExecuteNonQuery();
                }

                using var bulkCopy = new SqlBulkCopy(connection)
                {
                    DestinationTableName = "[dbo].[TaxiTrips]",
                    BatchSize = batchSize,
                    BulkCopyTimeout = 300
                };

                // Map DataTable columns to DB columns by name
                foreach (DataColumn column in table.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.WriteToServer(table);
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException(
                    $"Bulk insert failed. SQL Error: {ex.Number} — {ex.Message}",
                    ex);
            }

            Console.WriteLine("  Bulk insert complete.");
        }

        /// <summary>
        /// Returns the current row count from the TaxiTrips table.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the query fails.</exception>
        public int GetRowCount()
        {
            try
            {
                using var connection = new SqlConnection(_dbConnectionString);
                connection.Open();
                using var cmd = new SqlCommand("SELECT COUNT(*) FROM [dbo].[TaxiTrips]", connection);
                return (int)cmd.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException(
                    "Failed to retrieve row count from TaxiTrips table.",
                    ex);
            }
        }

        /// <summary>
        /// Builds a DataTable from the list of records, matching the DB schema.
        /// </summary>
        private static DataTable BuildDataTable(List<TaxiTripRecord> records)
        {
            var table = new DataTable();
            table.Columns.Add("PickupDateTimeUtc", typeof(DateTime));
            table.Columns.Add("DropoffDateTimeUtc", typeof(DateTime));
            table.Columns.Add("PassengerCount", typeof(byte));
            table.Columns.Add("TripDistance", typeof(decimal));
            table.Columns.Add("StoreAndFwdFlag", typeof(string));
            table.Columns.Add("PULocationID", typeof(short));
            table.Columns.Add("DOLocationID", typeof(short));
            table.Columns.Add("FareAmount", typeof(decimal));
            table.Columns.Add("TipAmount", typeof(decimal));

            foreach (var r in records)
            {
                table.Rows.Add(
                    r.PickupDateTime!.Value,
                    r.DropoffDateTime!.Value,
                    (byte)r.PassengerCount!.Value,
                    r.TripDistance!.Value,
                    r.StoreAndFwdFlag ?? string.Empty,
                    (short)r.PULocationID!.Value,
                    (short)r.DOLocationID!.Value,
                    r.FareAmount!.Value,
                    r.TipAmount!.Value
                );
            }

            return table;
        }

        /// <summary>
        /// Executes a SQL script against the given connection string.
        /// Splits on GO statements for multi-batch scripts.
        /// </summary>
        private static void ExecuteSql(string connectionString, string sql)
        {
            // Split script on GO batches (case-insensitive, must be on its own line)
            string[] batches = Regex.Split(
                sql,
                @"^\s*GO\s*$",
                RegexOptions.Multiline | RegexOptions.IgnoreCase);

            using var connection = new SqlConnection(connectionString);
            connection.Open();

            foreach (string batch in batches)
            {
                string trimmed = batch.Trim();
                if (string.IsNullOrEmpty(trimmed)) continue;

                using var cmd = new SqlCommand(trimmed, connection);
                cmd.ExecuteNonQuery();
            }
        }
    }
}
