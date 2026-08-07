#!/usr/bin/env python3
"""Provisions a Yeedu workspace from this repo: clone the repo in, register
each functions/ demo as a Yeedu Functions job, register each notebooks/
notebook.

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
import resolve_tenant_workspace  # noqa: E402
from yeedu_client import YeeduClient  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True, help="e.g. https://dev-onprem-008.yeedu.io:8080")
    parser.add_argument("--token", required=True, help="Pre-obtained Yeedu API token")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument(
        "--cluster-id", default=None,
        help="Optional. Without it, jobs/notebooks are created but not started.",
    )
    parser.add_argument("--git-branch", default="main")
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

    if args.insecure:
        print("WARNING: --insecure set — TLS certificate verification is disabled for this run.")
        os.environ["YEEDU_CLI_VERIFY_SSL"] = "false"

    client = YeeduClient(dry_run=args.dry_run)

    print("== Step 1/5: Auth ==")
    configure_auth.inject_token(args.api_url, args.token)
    configure_auth.smoke_test(client)

    print("\n== Step 2/5: Tenant + workspace ==")
    resolve_tenant_workspace.associate_tenant(client, args.tenant_id)
    resolve_tenant_workspace.resolve_workspace(client, args.workspace_id)

    print("\n== Step 3/5: Clone repo into workspace ==")
    clone_repo.clone_or_pull(client, args.workspace_id, git_branch=args.git_branch)

    print("\n== Step 4/5: Yeedu Functions jobs ==")
    jobs = deploy_functions.deploy_all(
        client, repo_root, args.workspace_id,
        cluster_id=args.cluster_id, start=args.start,
    )

    print("\n== Step 5/5: Notebooks ==")
    notebooks = create_notebooks.create_all(
        client, repo_root, args.workspace_id,
        cluster_id=args.cluster_id, skip_confirm=args.skip_notebook_confirm,
    )

    print("\n== Summary ==")
    print(f"Jobs: {len(jobs)} created/reused")
    for j in jobs:
        run_suffix = f" run_id={j['run_id']}" if j.get("run_id") else ""
        print(f"  - {j['job_name']}: job_id={j['job_id']}{run_suffix}")
    print(f"Notebooks: {len(notebooks)} created/reused this run")
    for n in notebooks:
        print(f"  - {n['notebook_name']}: notebook_id={n['notebook_id']}")


if __name__ == "__main__":
    main()
