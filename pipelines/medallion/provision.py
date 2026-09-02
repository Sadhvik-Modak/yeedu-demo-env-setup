#!/usr/bin/env python3
"""Provision the medallion banking demo into a Yeedu workspace.

Uploads the notebooks in ``notebooks/`` to the workspace file store, registers
each one as a Yeedu notebook, then creates the pipelines in ``definitions/``
with the real notebook ids substituted in.

Standard library only, and self-contained: it reads nothing outside this
directory.  Re-running is safe -- existing notebooks and pipelines are matched
by name and updated in place rather than duplicated.

    python3 provision.py --token "$YEEDU_TOKEN"
    python3 provision.py --token "$YEEDU_TOKEN" --dry-run

Nothing is ever executed: no notebook run and no pipeline trigger is issued.
"""

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
NOTEBOOK_DIR = os.path.join(HERE, "notebooks")
DEFINITION_DIR = os.path.join(HERE, "definitions")

DEFAULT_API_URL = "https://dev-onprem-009.yeedu.io:8080"
DEFAULT_TENANT = "c6b79638-ae11-428f-9a1f-246dfd3c6d01"
DEFAULT_WORKSPACE = 1151

# Workspace-relative directory the .ipynb files are uploaded into.
REMOTE_DIR = "medallion"

# Pipelines are created in this order so that `run_job_task` references to an
# earlier pipeline can be resolved to a real pipeline id.
PIPELINE_ORDER = [
    "medallion_banking_pipeline.json",
    "medallion_daily_orchestrator.json",
]


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

class Yeedu:
    """Thin client over the Yeedu v1 REST API."""

    def __init__(self, api_url, token, tenant_id, insecure=True, verbose=False):
        self.base = api_url.rstrip("/") + "/api/v1"
        self.token = token
        self.tenant_id = tenant_id
        self.verbose = verbose
        if insecure:
            self.ctx = ssl.create_default_context()
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
        else:
            self.ctx = ssl.create_default_context()

    def _headers(self, extra=None):
        headers = {
            "Authorization": "Bearer " + self.token,
            "TENANT-ID": self.tenant_id,
            "Accept": "application/json",
        }
        headers.update(extra or {})
        return headers

    def request(self, method, path, query=None, body=None, raw=None, headers=None):
        url = self.base + path
        if query:
            url += "?" + urllib.parse.urlencode(query)

        extra = dict(headers or {})
        if raw is not None:
            data = raw
            extra.setdefault("Content-Type", "application/octet-stream")
        elif body is not None:
            data = json.dumps(body).encode()
            extra.setdefault("Content-Type", "application/json")
        else:
            data = None

        req = urllib.request.Request(url, data=data, method=method,
                                     headers=self._headers(extra))
        if self.verbose:
            print(f"    {method} {url}")
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=180) as resp:
                payload = resp.read().decode()
                return resp.status, (json.loads(payload) if payload.strip() else {})
        except urllib.error.HTTPError as err:
            payload = err.read().decode()
            try:
                return err.code, json.loads(payload)
            except ValueError:
                return err.code, {"error": payload[:500]}

    def get(self, path, query=None):
        return self.request("GET", path, query=query)

    def post(self, path, body=None, query=None, raw=None, headers=None):
        return self.request("POST", path, query=query, body=body, raw=raw, headers=headers)

    def put(self, path, body=None, query=None):
        return self.request("PUT", path, query=query, body=body)


# --------------------------------------------------------------------------
# Cluster placement
# --------------------------------------------------------------------------

# Each notebook is attached to a cluster sized for the work it does, so the
# demo can point at task-level placement rather than one cluster for
# everything.  Pipeline tasks override this per task via `task_cluster_id`.
#
# Only clusters attached to the workspace may be referenced -- demo_workspace
# has 977/978/979 but not 980 (onprem_large), which returns RFA-000129.
NOTEBOOK_CLUSTER = {
    "00_setup_environment":         979,   # onprem_small  - DDL only
    "01_ingest_bronze_accounts":    977,   # onprem_medium - small dimension feed
    "02_ingest_bronze_transactions": 977,  # onprem_medium - high-volume feed
    "03_validate_bronze":           979,   # onprem_small  - counts and ratios
    "04_build_silver_dim_account":  978,   # aws_medium    - window dedupe
    "05_build_silver_txn_clean":    978,   # aws_medium    - the big join
    "06_validate_silver":           979,   # onprem_small  - counts and ratios
    "07_gold_account_balance_daily": 978,  # aws_medium    - windowed aggregation
    "08_gold_fraud_signal_summary": 977,   # onprem_medium - runs cross-cloud from its sibling
    "09_publish_and_audit":         979,   # onprem_small  - metadata only
    "10_quarantine_bad_records":    977,   # onprem_medium - exception handling
    "11_table_stats":               979,   # onprem_small  - profiling loop body
}


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------

def notebook_names():
    return sorted(
        f[: -len(".ipynb")]
        for f in os.listdir(NOTEBOOK_DIR)
        if f.endswith(".ipynb")
    )


def upload_notebooks(api, workspace_id, dry_run):
    """Upload each .ipynb into the workspace file store under REMOTE_DIR."""
    print(f"\n=== uploading {len(notebook_names())} notebooks to /{REMOTE_DIR} ===")
    for name in notebook_names():
        filename = name + ".ipynb"
        local = os.path.join(NOTEBOOK_DIR, filename)
        with open(local, "rb") as fh:
            content = fh.read()

        if dry_run:
            print(f"  [dry-run] upload {filename} ({len(content)} bytes)")
            continue

        # The path must be workspace-absolute and fully URL-encoded (urlencode
        # escapes the separators).  `target_dir` is rejected for a directory
        # that does not already exist, but a nested `path` creates it.
        status, body = api.post(
            "/workspace/files",
            query={
                "workspace_id": workspace_id,
                "path": f"/{REMOTE_DIR}/{filename}",
                "overwrite": "true",
            },
            raw=content,
            headers={"x-file-size": str(len(content))},
        )
        ok = status in (200, 201)
        print(f"  [{'ok ' if ok else status}] {filename:<38} {len(content):>6} bytes"
              + ("" if ok else f"  {body}"))
        if not ok:
            raise SystemExit(f"upload failed for {filename}: {status} {body}")


def existing_notebooks(api, workspace_id):
    status, body = api.get(f"/workspace/{workspace_id}/notebooks")
    if status != 200 or not isinstance(body, dict):
        return {}
    return {n["notebook_name"]: int(n["notebook_id"]) for n in body.get("data", [])}


def create_notebooks(api, workspace_id, dry_run):
    """Register each uploaded file as a Yeedu notebook. Returns name -> id."""
    print(f"\n=== registering notebooks in workspace {workspace_id} ===")
    already = {} if dry_run else existing_notebooks(api, workspace_id)
    ids = {}

    for name in notebook_names():
        cluster_id = NOTEBOOK_CLUSTER[name]
        payload = {
            "notebook_name": name,
            "notebook_type": "python3",
            "notebook_path": f"/{REMOTE_DIR}/{name}.ipynb",
            "cluster_id": cluster_id,
            "max_concurrency": 1,
        }

        if dry_run:
            print(f"  [dry-run] create {name}")
            print(f"            {json.dumps(payload)}")
            continue

        if name in already:
            ids[name] = already[name]
            print(f"  [skip] {name:<38} exists as notebook_id={already[name]}")
            continue

        status, body = api.post(f"/workspace/{workspace_id}/notebook", body=payload)
        if status not in (200, 201):
            raise SystemExit(f"notebook create failed for {name}: {status} {body}")
        notebook_id = body.get("notebook_id") or body.get("data", {}).get("notebook_id")
        # the create response returns the id as a string, the list endpoint as an
        # int -- the pipeline schema only accepts an int, so normalise here.
        notebook_id = int(notebook_id)
        ids[name] = notebook_id
        print(f"  [ok ] {name:<38} notebook_id={notebook_id}  cluster={cluster_id}")

    return ids


def resolve(node, notebook_ids, pipeline_ids):
    """Replace __notebook__ / __pipeline__ placeholders with real ids.

    ``{"__notebook__": "01_ingest_bronze_accounts"}`` on a task becomes
    ``"notebook_task": {"job_id": <id>}``; ``{"__pipeline__": "..."}`` inside a
    ``run_job_task`` becomes ``{"pipeline_id": <id>}``.
    """
    if isinstance(node, list):
        return [resolve(x, notebook_ids, pipeline_ids) for x in node]
    if not isinstance(node, dict):
        return node

    out = {}
    for key, value in node.items():
        if key == "__notebook__":
            if value not in notebook_ids:
                raise SystemExit(f"unknown notebook reference: {value}")
            out["notebook_task"] = {"job_id": notebook_ids[value]}
        elif key == "__pipeline__":
            if value not in pipeline_ids:
                raise SystemExit(f"unknown pipeline reference: {value}")
            out["pipeline_id"] = pipeline_ids[value]
        else:
            out[key] = resolve(value, notebook_ids, pipeline_ids)
    return out


def existing_pipelines(api, workspace_id):
    status, body = api.get(f"/workspace/{workspace_id}/pipelines")
    if status != 200 or not isinstance(body, dict):
        return {}
    return {p["name"]: int(p["pipeline_id"]) for p in body.get("data", []) if p.get("pipeline_id")}


def create_pipelines(api, workspace_id, notebook_ids, dry_run):
    print(f"\n=== creating pipelines in workspace {workspace_id} ===")
    already = {} if dry_run else existing_pipelines(api, workspace_id)
    pipeline_ids = dict(already)

    for filename in PIPELINE_ORDER:
        path = os.path.join(DEFINITION_DIR, filename)
        with open(path) as fh:
            definition = json.load(fh)

        name = definition["name"]
        payload = resolve(definition, notebook_ids, pipeline_ids)

        if dry_run:
            print(f"  [dry-run] create pipeline {name} ({len(payload['tasks'])} tasks)")
            print(json.dumps(payload, indent=2))
            continue

        if name in already:
            status, body = api.put(f"/workspace/{workspace_id}/pipeline/{already[name]}",
                                   body=payload)
            verb = "updated"
        else:
            status, body = api.post(f"/workspace/{workspace_id}/pipeline", body=payload)
            verb = "created"

        if status not in (200, 201):
            raise SystemExit(f"pipeline {verb[:-1]} failed for {name}: {status} {body}")

        # the create response does not reliably echo the new id, so fall back to
        # looking the pipeline up by name -- the orchestrator needs a real int
        # for its run_job_task.pipeline_id.
        pipeline_id = (body.get("pipeline_id")
                       or (body.get("data") or {}).get("pipeline_id")
                       or already.get(name))
        if pipeline_id is None:
            pipeline_id = existing_pipelines(api, workspace_id).get(name)
        if pipeline_id is None:
            raise SystemExit(f"could not determine pipeline_id for {name}")
        pipeline_ids[name] = int(pipeline_id)
        print(f"  [ok ] {name:<32} {verb} pipeline_id={pipeline_id} "
              f"({len(payload['tasks'])} tasks)")

    return pipeline_ids


def summarise(api, workspace_id, pipeline_ids):
    print(f"\n=== verification ===")
    status, body = api.get(f"/workspace/{workspace_id}/notebooks")
    if status == 200:
        mine = [n for n in body.get("data", []) if n["notebook_name"] in NOTEBOOK_CLUSTER]
        print(f"  notebooks registered : {len(mine)}/12")

    for name, pipeline_id in pipeline_ids.items():
        status, body = api.get(f"/workspace/{workspace_id}/pipeline/{pipeline_id}")
        if status == 200:
            data = body.get("data", body)
            tasks = data.get("tasks", [])
            print(f"  pipeline {name:<32} id={pipeline_id} tasks={len(tasks)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--token", default=os.environ.get("YEEDU_TOKEN"),
                        help="bearer token; defaults to $YEEDU_TOKEN")
    parser.add_argument("--tenant-id", default=DEFAULT_TENANT)
    parser.add_argument("--workspace-id", type=int, default=DEFAULT_WORKSPACE)
    parser.add_argument("--insecure", action="store_true", default=True,
                        help="skip TLS verification (the dev hosts use self-signed certs)")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the payloads that would be sent, change nothing")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not args.token and not args.dry_run:
        parser.error("--token is required (or set YEEDU_TOKEN)")

    api = Yeedu(args.api_url, args.token or "", args.tenant_id,
                insecure=args.insecure, verbose=args.verbose)

    print(f"api       : {api.base}")
    print(f"tenant    : {args.tenant_id}")
    print(f"workspace : {args.workspace_id}")
    print(f"mode      : {'DRY RUN' if args.dry_run else 'LIVE'}")

    upload_notebooks(api, args.workspace_id, args.dry_run)
    notebook_ids = create_notebooks(api, args.workspace_id, args.dry_run)

    if args.dry_run:
        # Placeholder ids so the pipeline payloads can still be rendered.
        notebook_ids = {name: 900000 + i for i, name in enumerate(notebook_names())}

    pipeline_ids = create_pipelines(api, args.workspace_id, notebook_ids, args.dry_run)

    if not args.dry_run:
        summarise(api, args.workspace_id, pipeline_ids)
        print("\nDone. Nothing was executed - no notebook runs, no pipeline triggers.")


if __name__ == "__main__":
    sys.exit(main())
