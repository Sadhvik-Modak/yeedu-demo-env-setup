"""Standalone Spark job (Yeedu job_type: Python3): prints a gold table's
row count and top rows.

Companion to the JAR (SparkPi) and SQL demos in ../jar/ and ../sql/ — same
"query a gold table" idea, different Yeedu job type. Requires the table to
already exist (see notebooks/data-transformation/).

Usage (via spark-submit, or Yeedu's job_command + job_arguments):
    gold_table_summary_job.py <database.table>

Example:
    gold_table_summary_job.py nyc_taxi.gold_taxi_trip_summary_v1
"""
import sys

from pyspark.sql import SparkSession


def main():
    if len(sys.argv) < 2:
        print("Usage: gold_table_summary_job.py <database.table>", file=sys.stderr)
        sys.exit(1)

    table_name = sys.argv[1]

    spark = SparkSession.builder.appName("GoldTableSummaryJob").getOrCreate()

    df = spark.table(table_name)
    row_count = df.count()

    print(f"Table: {table_name}")
    print(f"Row count: {row_count}")
    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
