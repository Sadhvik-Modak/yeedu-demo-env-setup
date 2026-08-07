-- Spark SQL job (Yeedu job_type: SQL): summarizes a business-metric table.
--
-- Companion to the JAR (SparkPi-successor) and Python demos in ../jar/ and
-- ../python/ -- same "query a business-metric table" idea, different
-- Yeedu job type. Requires the table to already exist (see
-- notebooks/data-transformation/).
--
-- Table is hardcoded here since a Spark SQL job runs a fixed script, not
-- a parameterized one with command-line arguments like the Python/JAR
-- demos.

SELECT *
FROM retail.customer_rfm_segments_v1
ORDER BY rfm_score DESC
LIMIT 10;
