package io.yeedu.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Standalone Spark job (Yeedu job_type: JAR): prints a table's row count
 * and top rows. Java/JAR equivalent of ../../python/table_summary_job.py
 * and ../../sql/table_summary.sql -- same "query a business-metric table"
 * idea, different Yeedu job type.
 *
 * Usage: TableSummaryJob &lt;database.table&gt;
 */
public class TableSummaryJob {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: TableSummaryJob <database.table>");
            System.exit(1);
        }
        String tableName = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("TableSummaryJob")
                .getOrCreate();

        Dataset<Row> df = spark.table(tableName);
        long rowCount = df.count();

        System.out.println("Table: " + tableName);
        System.out.println("Row count: " + rowCount);
        df.show(10, false);

        spark.stop();
    }
}
