"""Register each functions/ demo as a Yeedu Functions job."""
import os
import re

from clone_repo import REPO_WORKSPACE_PATH
from yeedu_client import find_exact_match, get_field, run_allow_not_found

FUNCTIONS_DEMOS = [
    {"dir": "iris", "function_name": "classify_plant_sample", "job_name": "iris_classify_plant_sample"},
    {"dir": "fraud-risk-scoring", "function_name": "score_claim", "job_name": "fraud_risk_scoring_score_claim"},
    {"dir": "sentiment-analysis", "function_name": "analyze_sentiment", "job_name": "sentiment_analysis_analyze_sentiment"},
]

# Strips everything from the first version-pin/marker character onward, so
# "pandas>=1.5,<3" -> "pandas". `--yeedu-functions-requirements` does a raw
# str.split(' ') under the hood (yeedu-cli skill gotcha #4) — any comma from
# a version pin shreds the value, so only bare space-separated names work.
_VERSION_PIN_RE = re.compile(r"[<>=!~\[; ].*$")


def _bare_requirements(requirements_txt_path):
    names = []
    with open(requirements_txt_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name = _VERSION_PIN_RE.sub("", line).strip()
            if name:
                names.append(name)
    return " ".join(names)


def _find_job_id(client, workspace_id, job_name):
    result = run_allow_not_found(
        client, "job", "search", "--workspace_id", str(workspace_id), "--job_name", job_name
    )
    # See yeedu_client.find_exact_match — `search` can return prefix
    # matches (confirmed live for `notebook search`); match jobs exactly
    # too rather than trusting result order.
    match = find_exact_match(result, "job_name", job_name)
    return match.get("job_id") if match else None


def deploy_all(client, repo_root, workspace_id, cluster_id=None, start=False):
    created = []
    for demo in FUNCTIONS_DEMOS:
        local_demo_dir = os.path.join(repo_root, "functions", demo["dir"])
        requirements = _bare_requirements(os.path.join(local_demo_dir, "requirements.txt"))
        with open(os.path.join(local_demo_dir, "config", "example_payload.json")) as f:
            example_payload = f.read()

        project_path = f"{REPO_WORKSPACE_PATH}/functions/{demo['dir']}"
        script_path = f"{project_path}/function.py"

        print(f"\n== Functions job: {demo['job_name']} ==")
        existing_job_id = _find_job_id(client, workspace_id, demo["job_name"])
        if existing_job_id:
            print(f"Job already exists (job_id={existing_job_id}) — reusing, skipping create.")
            job_id = existing_job_id
        else:
            args = [
                "job", "create",
                "--workspace_id", str(workspace_id),
                "--name", demo["job_name"],
                "--job-type", "Functions",
                "--yeedu-functions-project-path", project_path,
                "--yeedu-functions-script-path", script_path,
                "--yeedu-functions-function-name", demo["function_name"],
                "--yeedu-functions-requirements", requirements,
                "--yeedu-functions-example-request-body", example_payload,
            ]
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

        created.append({"job_name": demo["job_name"], "job_type": "Functions", "job_id": job_id, "run_id": run_id})
    return created
