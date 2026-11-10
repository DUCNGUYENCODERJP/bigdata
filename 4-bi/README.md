# Business Intelligence Dashboard

The web dashboard reads live data from Hive SQL tables through Spark SQL. It
does not load demo values, local CSV files, or Parquet files directly.

## Run

From the repository root in PowerShell:

```powershell
.\run_dashboard.ps1
```

By default, the server reads from the **oldest month available in both Hive
tables through the newest month available in both tables**. It discovers the
range from Hive partition metadata at startup, so the end date grows as new
monthly partitions are added. The dashboard includes a month-by-month trip
chart. Override the default to query one month:

```powershell
.\run_dashboard.ps1 -WarehouseHost 192.168.100.17 `
  -Partition year=2024/month=1 `
  -CleanTable default.clean_nyc_taxi_tlc `
  -RawTable default.raw_nyc_taxi
```

Required local prerequisites are JDK 17, Hadoop Windows binaries, PySpark,
NumPy, and network access to the Hive Metastore (9083) and HDFS (8020). The
aggregate dashboard and monthly trend cover the selected range. To keep
interactive forecasting practical, the `all` range trains GBT on the newest
available shared partition; selecting one month trains on that month. The model
window is shown in the dashboard. The server returns an explicit API error if
the Hive SQL query or model training fails; it does not substitute sample data.

Open the dashboard at `http://localhost:8080/dashboard.html`. A valid snapshot is
reused by warehouse host, partition, table names, and data shape; it does not
expire based on age. The default `all` snapshot lives in
`4-bi/cache/dashboard_hive_cache.json`; other query identities are cached under
`%LOCALAPPDATA%\NYCTaxiDashboard\cache`.

## Backend layout

The runnable entry point is `server.py`; implementation is split by responsibility
under `dashboard_backend/`:

| Module | Responsibility |
|---|---|
| `context.py` | Paths, runtime configuration, and shared process state |
| `cache.py` | Validate, load, and atomically save snapshots |
| `warehouse.py` | Stable warehouse operations imported by the other modules |
| `warehouse_metadata.py` | Partition discovery and HDFS metadata |
| `warehouse_query.py` | Spark SQL aggregates and ML forecast generation |
| `cache_merge.py` | Merge one monthly result into dashboard aggregates |
| `sync.py` | Poll warehouse metadata and append uncached months |
| `http_handler.py` | Static assets and dashboard API routes |

For the default `all` range, the server checks lightweight WebHDFS metadata every
5 seconds (configurable with `NYC_TAXI_CACHE_POLL_SECONDS`, minimum 3). A change
must be stable across four polls before Hive metadata is refreshed. The server then
queries only months absent from the durable cache and saves each month as soon as
its query completes. The dashboard updates quietly when the cache version changes;
there is no manual cache-sync button or success toast.

On a cold start, Hive partition metadata sets both range endpoints dynamically.
During operation, a newly published month present in both the clean and raw tables
is appended to the cache and extends the end month automatically. The lower bound
remains the oldest shared month. Power BI uses its separately generated workbook;
refresh that workbook after the monthly Spark/ML export is regenerated.

The cache treats a completed month as immutable: files appended to or replaced in
an already loaded month are not reread automatically. This preserves the
no-reread rule; the current source pipeline publishes monthly partitions. A cache
miss still requires one full Hive/HDFS query and model fit, so keep the project
snapshot available to avoid repeating the long initial scan.

## Power BI data source

Power BI imports the generated workbook `power_bi_data/NYC_Taxi_Full_History.xlsx`.
It contains compact Spark exports, model metrics, monthly raw/manifest
reconciliation and the TLC taxi-zone lookup; it never loads raw trip rows.
HiveServer2 at `192.168.100.17:10000` currently closes the client during session
startup, so the report does not depend on the Hive ODBC DSN. The Hive tables
remain the source for the Spark run that creates the workbook.

## Build the requested Power BI report

The Power Query and DAX source files are in `power_query/` and `dax/`. The exact
model relationships and visual field wells for the **zone map** and **weekday ×
hour heatmap** are in [`POWER_BI_BUILD_GUIDE.md`](POWER_BI_BUILD_GUIDE.md).
`01_taxi_zone_lookup.m` adds `MapLocation` so a Power BI map can geocode the zone
with its borough and city context. A zone bubble map is suitable for the
location-level requirement; a boundary-filled choropleth needs the official
TLC taxi-zone geometry as a custom GeoJSON/TopoJSON map.

Power BI Desktop is installed and launches. HiveServer2 currently closes the
client connection during `OpenSession`, so the report source is a local workbook
generated from Spark exports rather than an ODBC DSN. The full-history Spark
evaluation is running over 188 shared clean/raw partitions from 2011-01 through
2026-08. After it completes, package the result with
`3-analytics/package_power_bi_data.py`, refresh the report, and save
`4-bi/NYC_Taxi_Analytics.pbix`. Until that refresh and visual check finish, the
Power BI deliverable is still in progress.
