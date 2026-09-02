import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val inputPath = "s3a://cdpmodakbucket/Streaming/Input/"
val checkpointPath = "s3a://cdpmodakbucket/Streaming/Checkpoint/"
val schemaPath = "s3a://cdpmodakbucket/Streaming/Schema/"
val outputPath = "s3a://cdpmodakbucket/Streaming/Output/"

// incrementally discover and read new parquet files landing in inputPath
val df = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.schemaLocation", schemaPath) // where inferred/evolved schema is persisted
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.includeExistingFiles", "true") // also pick up files already present at start
  .option("cloudFiles.maxFilesPerTrigger", 200) // cap files read per micro-batch
  .option("cloudFiles.maxFileAge", "1 Month") // ignore files older than this
  .option("recursiveFileLookup", "true") // look into subfolders of inputPath too
  .option("pathGlobFilter", "*.parquet") // only match parquet files
  .load(inputPath)
  .withColumn("file_name", input_file_name()) // track source file per row

// runs once per micro-batch on a materialized (non-streaming) DataFrame
def processTransformation(batchDF: DataFrame, batchId: Long): Unit = {
  println(s"Processing batch $batchId")

  val transformedDF = batchDF
    .withColumn("revenue_tier", // bucket revenue into LOW/MID/HIGH
      when(col("revenue_usd") < 25000, "LOW")
        .when(col("revenue_usd") < 100000, "MID")
        .otherwise("HIGH"))
    .withColumn("system_health_status", // flag infra health from cpu usage
      when(col("cpu_pct") >= 90, "CRITICAL")
        .when(col("cpu_pct") >= 75, "WARNING")
        .otherwise("HEALTHY"))
    .withColumn("vitals_alert", // flag out-of-range vitals for follow-up
      col("vital_bp_systolic") > 140 || col("vital_bp_diastolic") > 90 ||
      col("vital_heart_rate") < 60 || col("vital_heart_rate") > 100)
    .withColumn("patient_id_masked", concat(substring(col("patient_id"), 1, 4), lit("***"))) // partial mask
    .drop("patient_id") // drop the unmasked identifier
    .withColumn("processed_at", current_timestamp()) // audit timestamp

  transformedDF.write.mode("append").parquet(outputPath)
}

// drive the stream: one micro-batch every 5 minutes, checkpointed for exactly-once processing
df.writeStream
  .outputMode("append")
  .option("checkpointLocation", checkpointPath)
  .trigger(Trigger.ProcessingTime("5 minutes"))
  .foreachBatch(processTransformation _)
  .start()
  .awaitTermination()
