-- Create TaxiTripsDb database on LocalDB if it does not already exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'TaxiTripsDb')
BEGIN
    CREATE DATABASE [TaxiTripsDb];
END
GO
