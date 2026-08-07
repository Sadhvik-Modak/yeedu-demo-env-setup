# JAR Job: Table Summary

Demonstrates a Yeedu `job_type: JAR` job. `table-summary-job-1.0.jar` is
committed straight into this repo — same as everything else here, it
gets pulled into the workspace by the `git clone` step, so the job config
just points at the same in-workspace path convention as the Python/SQL
demos (`{REPO_WORKSPACE_PATH}/jobs/jar/table-summary-job-1.0.jar`), no
separate upload step and no dependency on whatever a given Yeedu instance
happens to have vendored internally.

`TableSummaryJob` (Java) is the JAR equivalent of
`../python/table_summary_job.py` and `../sql/table_summary.sql` — same
"query a business-metric table" idea, different job type: prints a
table's row count and top 10 rows. Takes the table name as its one
argument (`job_arguments`).

## Deploy config

**Confirmed live (2026-08-07):** `job_command` = workspace path to the
jar (no `file://` scheme — that's only for paths on Yeedu's own internal
filesystem, not needed for a workspace-relative path).

```
job_type:       JAR
job_command:    <workspace path to table-summary-job-1.0.jar>
job_class_name: io.yeedu.demo.TableSummaryJob
job_arguments:  retail.customer_rfm_segments_v1
```

Requires `retail.customer_rfm_segments_v1` to already exist — run
`notebooks/data-generators/retail_order_ingest.ipynb` and
`notebooks/data-transformation/customer_rfm_segmentation.ipynb` first.

## Why a thin jar

Spark itself stopped publishing `spark-examples` as a standalone Maven
Central artifact (confirmed: 404 across every version checked) — it only
ships bundled inside the full multi-hundred-MB Spark binary distribution,
too heavy to vendor for a demo. Instead: `pom.xml` declares
`spark-core`/`spark-sql` as `provided` scope (the cluster supplies them at
`spark-submit` time), so `mvn package` produces a jar containing only our
one compiled class — **3.2 KB**.

Compiled targeting Java 8 bytecode (`maven.compiler.target=1.8`) for
broad compatibility across likely cluster JVM versions — forward-
compatible with 11/17 runtimes.

## Rebuilding

```bash
cd jobs/jar
mvn package
cp target/table-summary-job-1.0.jar .
```

`<spark.version>`/`<scala.binary.version>` in `pom.xml` should match your
target cluster's `spark_infra_version` closely enough that the Spark APIs
used (a stable, ancient subset: `SparkSession.builder()`, `.table()`,
`.count()`, `.show()`) resolve correctly — not version-critical since
`provided` scope means only compile-time API surface matters, not runtime
bytecode compatibility with the cluster's actual Spark jars.

## Status

Config creation confirmed live (`job create` accepted this shape).
Execution is still unverified — needs a cluster (see
`automation/README.md` "Known gaps").
