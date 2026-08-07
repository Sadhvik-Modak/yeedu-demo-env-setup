#!/usr/bin/env python3
"""Provisions a Yeedu workspace from this repo: clone the repo in, register
each functions/ demo as a Yeedu Functions job, register each jobs/ demo
(Jar/Python3/SQL), register each notebooks/ notebook.

Targets Yeedu platform 2.10.1 via the `yeedu` CLI (see
automation/README.md for why, and for the assumptions this script makes
that were NOT independently verified against a live instance during
development — read "Known gaps" there before a real run).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import clone_repo  # noqa: E402
import configure_auth  # noqa: E402
import create_notebooks  # noqa: E402
import deploy_functions  # noqa: E402
import deploy_other_jobs  # noqa: E402
import resolve_tenant_workspace  # noqa: E402
from yeedu_client import YeeduClient  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True, help="e.g. https://dev-onprem-008.yeedu.io:8080")
    parser.add_argument("--token", default=None, help="Pre-obtained Yeedu API token")
    parser.add_argument("--username", default=None, help="Alternative to --token: username for `yeedu configure` login")
    parser.add_argument("--password", default=None, help="Alternative to --token: password for `yeedu configure` login")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument(
        "--workspace-id", default=None,
        help="Optional. If omitted, a new workspace is created for this run.",
    )
    parser.add_argument(
        "--workspace-name", default=None,
        help="Name for the auto-created workspace when --workspace-id is omitted (default: yeedu-demo-<timestamp>).",
    )
    parser.add_argument(
        "--cluster-id", default=None,
        help="Optional. Without it, jobs/notebooks are created but not started.",
    )
    parser.add_argument("--git-branch", default="main")
    parser.add_argument(
        "--spark-examples-jar", default=None,
        help=(
            "file:// path to Yeedu's vendored spark-examples jar for the "
            "jar_spark_pi demo (default: "
            f"{deploy_other_jobs.DEFAULT_SPARK_EXAMPLES_JAR!r}, Spark 3.2.2/"
            "Scala 2.12 — override if your target cluster runs a different "
            "spark_infra_version)."
        ),
    )
    parser.add_argument(
        "--start", action="store_true",
        help="Start Functions jobs after creating them (requires --cluster-id).",
    )
    parser.add_argument(
        "--skip-notebook-confirm", action="store_true",
        help="Skip the interactive pause after creating the first notebook.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print every `yeedu` command without executing anything.",
    )
    parser.add_argument(
        "--insecure", action="store_true",
        help=(
            "Set YEEDU_CLI_VERIFY_SSL=false (skip TLS cert verification). "
            "Needed for on-prem/dev hosts with self-signed certs — confirmed "
            "live: `yeedu` exits 255 with 'Please set the environment "
            "variable: YEEDU_SSL_CERT_FILE' otherwise. This weakens TLS "
            "verification; only use it for a known dev/QA sandbox. For a "
            "real cert, set YEEDU_SSL_CERT_FILE yourself instead and omit "
            "this flag."
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    if not args.token and not (args.username and args.password):
        raise SystemExit("Provide either --token, or both --username and --password.")

    if args.insecure:
        print("WARNING: --insecure set — TLS certificate verification is disabled for this run.")
        os.environ["YEEDU_CLI_VERIFY_SSL"] = "false"

    client = YeeduClient(dry_run=args.dry_run)

    print("== Step 1/6: Auth ==")
    if args.token:
        configure_auth.inject_token(args.api_url, args.token)
    else:
        configure_auth.login_with_credentials(
            args.api_url, args.username, args.password, dry_run=args.dry_run
        )
    configure_auth.smoke_test(client)

    print("\n== Step 2/6: Tenant + workspace ==")
    resolve_tenant_workspace.associate_tenant(client, args.tenant_id)
    if args.workspace_id:
        workspace_id = resolve_tenant_workspace.resolve_workspace(client, args.workspace_id)
    else:
        workspace_id = resolve_tenant_workspace.create_workspace(client, name=args.workspace_name)

    print("\n== Step 3/6: Clone repo into workspace ==")
    clone_repo.clone_or_pull(client, workspace_id, git_branch=args.git_branch)

    print("\n== Step 4/6: Yeedu Functions jobs ==")
    function_jobs = deploy_functions.deploy_all(
        client, repo_root, workspace_id,
        cluster_id=args.cluster_id, start=args.start,
    )

    print("\n== Step 5/6: Other job types (Jar / Python3 / SQL) ==")
    other_jobs = deploy_other_jobs.deploy_all(
        client, repo_root, workspace_id,
        cluster_id=args.cluster_id, start=args.start,
        spark_examples_jar=args.spark_examples_jar,
    )

    print("\n== Step 6/6: Notebooks ==")
    notebooks = create_notebooks.create_all(
        client, repo_root, workspace_id,
        cluster_id=args.cluster_id, skip_confirm=args.skip_notebook_confirm,
    )

    all_jobs = function_jobs + other_jobs

    print("\n== Summary ==")
    print(f"Workspace: {workspace_id}")
    print(f"Jobs: {len(all_jobs)} created/reused")
    for j in all_jobs:
        run_suffix = f" run_id={j['run_id']}" if j.get("run_id") else ""
        job_type_prefix = f"[{j['job_type']}] " if j.get("job_type") else ""
        print(f"  - {job_type_prefix}{j['job_name']}: job_id={j['job_id']}{run_suffix}")
    print(f"Notebooks: {len(notebooks)} created/reused this run")
    for n in notebooks:
        print(f"  - {n['notebook_name']}: notebook_id={n['notebook_id']}")


if __name__ == "__main__":
    main()
