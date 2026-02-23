-- Create TaxiTrips table with optimised schema and indexes
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TaxiTrips]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[TaxiTrips]
    (
        [Id]                 INT            IDENTITY(1,1) NOT NULL,
        [PickupDateTimeUtc]  DATETIME2(0)   NOT NULL,
        [DropoffDateTimeUtc] DATETIME2(0)   NOT NULL,
        [PassengerCount]     TINYINT        NOT NULL,
        [TripDistance]       DECIMAL(8,2)   NOT NULL,
        [StoreAndFwdFlag]    VARCHAR(3)     NOT NULL,
        [PULocationID]       SMALLINT       NOT NULL,
        [DOLocationID]       SMALLINT       NOT NULL,
        [FareAmount]         DECIMAL(10,2)  NOT NULL,
        [TipAmount]          DECIMAL(10,2)  NOT NULL,
        -- Persisted computed column for travel-time queries
        [TravelTimeSeconds]  AS DATEDIFF(SECOND, [PickupDateTimeUtc], [DropoffDateTimeUtc]) PERSISTED,

        CONSTRAINT [PK_TaxiTrips] PRIMARY KEY CLUSTERED ([Id] ASC)
    );

    -- Query 1: Find PULocationID with highest average tip_amount
    CREATE NONCLUSTERED INDEX [IX_TaxiTrips_PULocationID_TipAmount]
        ON [dbo].[TaxiTrips] ([PULocationID])
        INCLUDE ([TipAmount]);

    -- Query 2: Top 100 longest fares by trip_distance
    CREATE NONCLUSTERED INDEX [IX_TaxiTrips_TripDistance]
        ON [dbo].[TaxiTrips] ([TripDistance] DESC)
        INCLUDE ([PickupDateTimeUtc], [DropoffDateTimeUtc], [PassengerCount],
                 [StoreAndFwdFlag], [PULocationID], [DOLocationID],
                 [FareAmount], [TipAmount], [TravelTimeSeconds]);

    -- Query 3: Top 100 longest fares by travel time
    CREATE NONCLUSTERED INDEX [IX_TaxiTrips_TravelTime]
        ON [dbo].[TaxiTrips] ([TravelTimeSeconds] DESC)
        INCLUDE ([PickupDateTimeUtc], [DropoffDateTimeUtc], [PassengerCount],
                 [TripDistance], [StoreAndFwdFlag], [PULocationID],
                 [DOLocationID], [FareAmount], [TipAmount]);

    -- Query 4: General searches filtered by PULocationID (wide covering index)
    CREATE NONCLUSTERED INDEX [IX_TaxiTrips_PULocationID_All]
        ON [dbo].[TaxiTrips] ([PULocationID])
        INCLUDE ([PickupDateTimeUtc], [DropoffDateTimeUtc], [PassengerCount],
                 [TripDistance], [StoreAndFwdFlag], [DOLocationID],
                 [FareAmount], [TipAmount], [TravelTimeSeconds]);
END
GO
