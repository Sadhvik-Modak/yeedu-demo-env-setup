# JAR Job: SparkPi

Demonstrates a Yeedu `job_type: Jar` job — no custom build required.

## Why no JAR is committed here

Yeedu's own OpenAPI documentation shows its platform already vendors Spark's
bundled examples JAR internally, referenced by a local file URI:

```
job_class_name: org.apache.spark.examples.SparkPi
job_command:    file:///yeedu/object-storage-manager/spark-examples_2.12-3.2.2.jar
job_arguments:  "1000"
```

(Apache Spark itself stopped publishing `spark-examples` as a standalone
Maven Central artifact some time ago — it only ships bundled inside the
full Spark binary distribution — so this vendored copy is the practical
way to get a working JAR job without a multi-hundred-MB download or
standing up a JVM build toolchain just for a demo.)

`SparkPi` estimates π via Monte Carlo sampling — the classic "does this
Spark job type even work" smoke test, entirely self-contained (no data
dependency).

## Deploying your own JAR instead

Same job type, different artifact:
1. Upload your `.jar` into the workspace (`yeedu workspace
   create-workspace-file --local_file_path <jar>`) or reference one already
   on the cluster's filesystem.
2. Set `job_command` to that file's path (a `file://` URI) and
   `job_class_name` to your JAR's main class.
3. `job_arguments` are passed straight through as your `main(String[]
   args)` arguments.

## Version note

The exact vendored filename (`spark-examples_2.12-3.2.2.jar`) is tied to a
specific Spark/Scala build (Spark 3.2.2, Scala 2.12) — match it to your
target cluster's `spark_infra_version` (check via `yeedu resource
list-spark-infra-versions` or the cluster's config) or the JAR class won't
be found. `automation/deploy_other_jobs.py` exposes this as a parameter
rather than hardcoding it for that reason.
