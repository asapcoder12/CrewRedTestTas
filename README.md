# ETL Pipeline — Notes

## Row Count

After running the program, the `TaxiTrips` table contains **29,840 rows**.

- 30,000 records parsed from the CSV
- 49 records dropped due to missing required fields
- 111 duplicate records removed (written to `duplicates.csv`)

## Assumptions

- **Timezone**: The input datetimes are assumed to be in the US Eastern timezone (EST/EDT). They are converted to UTC before insertion into the database, as reflected by the column names `PickupDateTimeUtc` and `DropoffDateTimeUtc`.
- **Duplicate definition**: Duplicates are identified by the combination of `pickup_datetime`, `dropoff_datetime`, and `passenger_count`. The first occurrence is kept; subsequent matches are removed and written to `duplicates.csv`.
- **Full refresh**: Each run truncates the table and reloads all data. This makes the process idempotent — running it multiple times with the same CSV produces the same result.
- **Required fields**: Records with any missing value in the 9 selected columns are dropped. The model uses nullable types to gracefully parse incomplete rows rather than failing on them.
- **store_and_fwd_flag**: Values other than `Y` or `N` (after trimming and uppercasing) are preserved as-is rather than rejected.
- **LocalDB availability**: The program assumes SQL Server LocalDB is installed and the default instance `(localdb)\MSSQLLocalDB` is available.

## Full-Refresh ETL Design

The program truncates the `TaxiTrips` table before each import. This is a deliberate design choice — the CSV file is the source of truth, and the database table is a derived view of it. Truncating ensures the table always reflects exactly what is in the current CSV file, with no risk of stale or duplicate rows from previous runs. This makes the process idempotent: running it multiple times on the same CSV always produces the same result.

An alternative would be an incremental (append/merge) approach, but that would require change detection logic, merge statements, and potentially a staging table — significantly more complexity with no benefit for a single-file import. If the use case evolved to accumulate data from multiple CSV files over time, the design would be changed to use a staging table with `MERGE` statements for upserts instead of a full truncate.

`TRUNCATE` itself is extremely fast (it deallocates data pages without logging individual row deletions), so there is no meaningful performance cost.

## Scalability — Handling a 10 GB CSV Input File

If I knew the program would be used for a 10 GB file, I would make several changes:

1. **Stream instead of loading into memory.** I would replace the `List<T>` approach with an `IDataReader` adapter wrapping CsvHelper, feeding rows directly into `SqlBulkCopy`. This would keep memory usage constant regardless of file size.
2. **Increase batch size and enable streaming.** I would set `SqlBulkCopy.BatchSize` to 100,000+ and enable `SqlBulkCopy.EnableStreaming = true`. I would also use `SqlBulkCopyOptions.TableLock` to reduce lock contention and improve throughput.
3. **Drop indexes before insert, rebuild after.** Inserting millions of rows into a table with existing indexes is expensive. I would drop them first and rebuild with `SORT_IN_TEMPDB = ON` for faster index creation.
4. **External deduplication.** The in-memory `HashSet` approach would not scale to billions of keys. I would use a staging table with `GROUP BY` or an external-merge-sort to deduplicate without holding all keys in memory.
5. **Partition the target table.** I would partition by date range (e.g., monthly) so that concurrent inserts and queries operate on separate data pages, reducing contention.
