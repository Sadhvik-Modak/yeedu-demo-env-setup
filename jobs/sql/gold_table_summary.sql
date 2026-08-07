-- Spark SQL job (Yeedu job_type: SQL): summarizes a gold table.
--
-- Companion to the JAR (SparkPi) and Python demos in ../jar/ and
-- ../python/ -- same "query a gold table" idea, different Yeedu job type.
-- Requires the table to already exist (see notebooks/data-transformation/).
--
-- Table is hardcoded here since a Spark SQL job runs a fixed script, not
-- a parameterized one with command-line arguments like the Python/JAR
-- demos.

SELECT *
FROM nyc_taxi.gold_taxi_trip_summary_v1
ORDER BY pickup_date DESC
LIMIT 10;
