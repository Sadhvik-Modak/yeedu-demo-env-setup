# Yeedu Demo Notebooks

Notebooks that stock a Yeedu workspace with real Spark tables so demos
(dashboards, jobs, Functions) always have data to point at, instead of
needing ad-hoc setup each time.

Three-stage, medallion-style pattern:

- **[`data-generators/`](data-generators/)** — one notebook per source
  dataset. Downloads or synthesizes raw data, creates the Spark database if
  needed, and persists it as a managed table (`bronze` layer, as-is from
  source). Naming: `bronze_ingest_<source>.ipynb`.
- **[`data-transformation/`](data-transformation/)** — one notebook per
  derived dataset. Reads a generator's bronze table, cleans/aggregates it,
  and persists a derived managed table (`silver`/`gold` layer) for demo
  consumption. Naming: `silver_clean_<entity>_v<n>.ipynb` /
  `gold_<metric>_v<n>.ipynb`. Each transformation may also have a `_sql`
  companion (`gold_<metric>_v<n>_sql.ipynb`) — same result, written with
  `%%sql` cells instead of the DataFrame API, for demos that want to show
  Yeedu's SQL notebook experience.
- **[`visualization/`](visualization/)** — notebooks that chart the gold
  tables (and, for live sources, the raw feed directly) using Plotly. See
  its own section below.

Each notebook is self-contained and re-runnable (`CREATE DATABASE IF NOT
EXISTS` + `mode("overwrite")`), with a config cell at the top so the
database/table names and source can be changed without touching the logic
below.

## Demos

### NYC Taxi
- Generator: [`data-generators/bronze_ingest_nyc_taxi.ipynb`](data-generators/bronze_ingest_nyc_taxi.ipynb)
  — downloads a public NYC Yellow Taxi trip-record parquet file and persists
  it as `nyc_taxi.yellow_tripdata`.
- Transformation: [`data-transformation/gold_taxi_trip_summary_v1.ipynb`](data-transformation/gold_taxi_trip_summary_v1.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/gold_taxi_trip_summary_v1_sql.ipynb`](data-transformation/gold_taxi_trip_summary_v1_sql.ipynb)
  (`%%sql` cells) — both aggregate `nyc_taxi.yellow_tripdata` into a
  per-pickup-date summary (trip count, total revenue, avg distance, avg
  fare) and persist it as `nyc_taxi.gold_taxi_trip_summary_v1`.

### Citi Bike
- Generator: [`data-generators/bronze_ingest_citibike.ipynb`](data-generators/bronze_ingest_citibike.ipynb)
  — downloads a monthly Citi Bike (NYC) trip-data zip, extracts the CSV(s),
  and persists it as `citibike.tripdata`.
- Transformation: [`data-transformation/gold_citibike_station_activity_v1.ipynb`](data-transformation/gold_citibike_station_activity_v1.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/gold_citibike_station_activity_v1_sql.ipynb`](data-transformation/gold_citibike_station_activity_v1_sql.ipynb)
  (`%%sql` cells) — both aggregate `citibike.tripdata` by start station
  (total rides, member vs. casual split, avg trip duration) and persist it
  as `citibike.gold_station_activity_v1`.

### Wikipedia Clickstream
- Generator: [`data-generators/bronze_ingest_wikipedia_clickstream.ipynb`](data-generators/bronze_ingest_wikipedia_clickstream.ipynb)
  — downloads a monthly Wikipedia Clickstream TSV (English Wikipedia by
  default: referrer → page, request count) and persists it as
  `wikipedia.clickstream_enwiki`.
- Transformation: [`data-transformation/gold_wikipedia_clickstream_top_pages_v1.ipynb`](data-transformation/gold_wikipedia_clickstream_top_pages_v1.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/gold_wikipedia_clickstream_top_pages_v1_sql.ipynb`](data-transformation/gold_wikipedia_clickstream_top_pages_v1_sql.ipynb)
  (`%%sql` cells) — both aggregate `wikipedia.clickstream_enwiki` by
  destination page (total clicks, distinct referrer count) and persist it
  as `wikipedia.gold_clickstream_top_pages_v1`.

### Earthquakes (USGS)
- Generator: [`data-generators/bronze_ingest_usgs_earthquakes.ipynb`](data-generators/bronze_ingest_usgs_earthquakes.ipynb)
  — downloads the USGS "past month" earthquake GeoJSON feed, flattens it,
  and persists it as `earthquake_data.earthquakes`. This feed updates
  continuously, so re-running pulls fresh data.
- Transformation: [`data-transformation/gold_earthquake_daily_summary_v1.ipynb`](data-transformation/gold_earthquake_daily_summary_v1.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/gold_earthquake_daily_summary_v1_sql.ipynb`](data-transformation/gold_earthquake_daily_summary_v1_sql.ipynb)
  (`%%sql` cells) — both aggregate `earthquake_data.earthquakes` by event
  date (total quakes, avg/max magnitude, magnitude-4.0+ count) and persist
  it as `earthquake_data.gold_earthquake_daily_summary_v1`.

### Weather (NOAA GSOD)
- Generator: [`data-generators/bronze_ingest_noaa_weather.ipynb`](data-generators/bronze_ingest_noaa_weather.ipynb)
  — downloads a year of NOAA GSOD daily observations for one station (AWS
  Open Data, no auth) and persists it as `noaa_weather.daily_observations`,
  including NOAA's raw missing-value sentinels.
- Transformation: [`data-transformation/gold_weather_monthly_summary_v1.ipynb`](data-transformation/gold_weather_monthly_summary_v1.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/gold_weather_monthly_summary_v1_sql.ipynb`](data-transformation/gold_weather_monthly_summary_v1_sql.ipynb)
  (`%%sql` cells) — both null out the missing-value sentinels and aggregate
  `noaa_weather.daily_observations` by month (avg/max/min temp, total
  precip, avg wind speed), persisting it as
  `noaa_weather.gold_weather_monthly_summary_v1`.

## Visualization

[`visualization/`](visualization/) — chart notebooks, using
[Plotly](https://plotly.com/python/) (interactive, polished defaults,
built-in maps, no extra Jupyter widget extensions needed):

- [`realtime_earthquake_activity.ipynb`](visualization/realtime_earthquake_activity.ipynb)
  — polls the USGS "past hour" feed directly (no Spark table needed) on a
  fixed interval and re-renders a live world map each time, magnitude
  driving color and size. A genuine live-refresh demo, not a canned
  animation.
- [`gold_tables_dashboard.ipynb`](visualization/gold_tables_dashboard.ipynb)
  — one chart per dataset's gold table (taxi revenue trend, Citi Bike top
  stations, Wikipedia top pages, earthquake daily activity, weather
  monthly range), styled consistently for a quick "what's in this demo
  environment" walkthrough.

Dependencies: `visualization/requirements.txt` (`plotly`, `pandas`).

## Adding a new dataset

1. Add a `bronze_ingest_<source>.ipynb` under `data-generators/` following
   the NYC Taxi notebook's shape: config cell → download/synthesize →
   load → `CREATE DATABASE IF NOT EXISTS` → `saveAsTable` (overwrite) →
   verify (count + describe).
2. If the demo needs cleaned/aggregated data on top, add a
   `silver_clean_<entity>_v<n>.ipynb` or `gold_<metric>_v<n>.ipynb` under
   `data-transformation/` that reads the bronze table, transforms it, and
   persists the result the same way.
3. Add an entry under **Demos** above.
