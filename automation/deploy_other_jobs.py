"""Register the jobs/{jar,python,sql} demos as Yeedu jobs (job_type
JAR/Python/Spark SQL). Companion to deploy_functions.py (job_type
Functions).

CONFIRMED LIVE (2026-08-07): `job_type: JAR` and `job_type: Python` both
use `job_command` = workspace path to the entrypoint (jar/py) — same
`{REPO_WORKSPACE_PATH}/...` convention as everything else cloned into the
workspace by clone_repo.py, no `file://` scheme needed (that scheme was
only for Yeedu's own internal filesystem paths, e.g. its vendored
spark-examples jar — not used here since the JAR is committed to this repo
and cloned in like everything else, per user request). `job_type: Spark
SQL` does NOT use `job_command` — it rejects it and instead requires
`--job-raw-scala-code` (confusingly named for a SQL job) set to a **local
filesystem path** (read client-side by the `yeedu` CLI itself, NOT a
workspace path and NOT the SQL text inline — confirmed via two rejections:
first "Please provide 'job_rawScalaCode'...", then, after passing the raw
SQL text directly, "The file cannot be found at '<the SQL text>' for the
argument --job_raw_scala_code"). See jobs/*/README.md and
automation/README.md "Confirmed live" for the full story.
"""
import os

from clone_repo import REPO_WORKSPACE_PATH
from yeedu_client import find_exact_match, get_field, run_allow_not_found

DEFAULT_TABLE = "retail.customer_rfm_segments_v1"

JOB_DEMOS = [
    {
        "job_name": "jar_table_summary",
        "job_type": "JAR",
        "job_command_rel_path": "jobs/jar/table-summary-job-1.0.jar",
        "job_class_name": "io.yeedu.demo.TableSummaryJob",
        "job_arguments": DEFAULT_TABLE,
    },
    {
        "job_name": "python_table_summary",
        "job_type": "Python",
        "job_command_rel_path": "jobs/python/table_summary_job.py",
        "job_arguments": DEFAULT_TABLE,
    },
    {
        "job_name": "sql_table_summary",
        "job_type": "Spark SQL",
        "sql_file_rel_path": "jobs/sql/table_summary.sql",
    },
]


def _find_job_id(client, workspace_id, job_name):
    result = run_allow_not_found(
        client, "job", "search", "--workspace_id", str(workspace_id), "--job_name", job_name
    )
    match = find_exact_match(result, "job_name", job_name)
    return match.get("job_id") if match else None


def deploy_all(client, repo_root, workspace_id, cluster_id=None, start=False):
    created = []
    for demo in JOB_DEMOS:
        job_name = demo["job_name"]
        print(f"\n== Job ({demo['job_type']}): {job_name} ==")

        existing_job_id = _find_job_id(client, workspace_id, job_name)
        if existing_job_id:
            print(f"Job already exists (job_id={existing_job_id}) — reusing, skipping create.")
            job_id = existing_job_id
        else:
            args = [
                "job", "create",
                "--workspace_id", str(workspace_id),
                "--name", job_name,
                "--job-type", demo["job_type"],
            ]
            if demo["job_type"] == "Spark SQL":
                # --job-raw-scala-code takes a LOCAL file path, read
                # client-side by the CLI itself — confirmed live (see
                # module docstring). Not a workspace path, not inline text.
                sql_path = os.path.join(repo_root, demo["sql_file_rel_path"])
                args += ["--job-raw-scala-code", sql_path]
            else:
                args += ["--job-command", f"{REPO_WORKSPACE_PATH}/{demo['job_command_rel_path']}"]
            if demo.get("job_class_name"):
                args += ["--job-class-name", demo["job_class_name"]]
            if demo.get("job_arguments"):
                args += ["--job-arguments", demo["job_arguments"]]
            if cluster_id:
                args += ["--cluster_id", str(cluster_id)]
            result = client.run(*args)
            job_id = get_field(result, "job_id", default="<dry-run-job-id>")
            print(f"Created job_id={job_id}")

        run_id = None
        if start and cluster_id:
            print(f"Starting job_id={job_id}...")
            start_result = client.run("job", "start", "--job_id", str(job_id), "--workspace_id", str(workspace_id))
            run_id = get_field(start_result, "run_id")
            print(f"Started — run_id={run_id}")
        elif start and not cluster_id:
            print("--start requested but no --cluster-id given — skipping start (jobs need a cluster to run on).")

        created.append({"job_name": job_name, "job_type": demo["job_type"], "job_id": job_id, "run_id": run_id})
    return created
