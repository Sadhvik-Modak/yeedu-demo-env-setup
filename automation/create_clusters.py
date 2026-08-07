#!/usr/bin/env python3
"""Creates S/M/L/XL Yeedu cluster tiers on an OnPrem environment —
never starts them. See deploy_clusters.py for the full story (confirmed
live gotchas, defaults, idempotency).

Separate from provision.py: this targets cluster/cloud-env infra, not a
specific workspace, and has no repo-clone step.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import configure_auth  # noqa: E402
import deploy_clusters  # noqa: E402
import resolve_tenant_workspace  # noqa: E402
from yeedu_client import YeeduClient  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True, help="e.g. https://dev-onprem-005.yeedu.io:8080")
    parser.add_argument("--token", default=None, help="Pre-obtained Yeedu API token")
    parser.add_argument("--username", default=None, help="Alternative to --token: username for `yeedu configure` login")
    parser.add_argument("--password", default=None, help="Alternative to --token: password for `yeedu configure` login")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument(
        "--cluster-username", default=deploy_clusters.DEFAULT_USERNAME,
        help="Username for the OnPrem (Proxmox Basic Auth) credential config this script creates.",
    )
    parser.add_argument(
        "--cluster-password", default=deploy_clusters.DEFAULT_PASSWORD,
        help=(
            "Password for the credential config. Not retrievable from any existing config "
            f"(write-only) — defaults to an obvious placeholder ({deploy_clusters.DEFAULT_PASSWORD!r}). "
            "Pass a real one, or edit the credential config afterward before starting any cluster."
        ),
    )
    parser.add_argument("--endpoint", default=deploy_clusters.DEFAULT_ENDPOINT)
    parser.add_argument("--onprem-node-names", default=deploy_clusters.DEFAULT_ONPREM_NODE_NAMES)
    parser.add_argument("--boot-disk-image", default=deploy_clusters.DEFAULT_BOOT_DISK_IMAGE)
    parser.add_argument("--spark-infra-version-id", default=deploy_clusters.DEFAULT_SPARK_INFRA_VERSION_ID)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print every `yeedu` command without executing anything.",
    )
    parser.add_argument(
        "--insecure", action="store_true",
        help="Set YEEDU_CLI_VERIFY_SSL=false (skip TLS cert verification) — only for a known dev/QA sandbox.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.token and not (args.username and args.password):
        raise SystemExit("Provide either --token, or both --username and --password.")

    if args.insecure:
        print("WARNING: --insecure set — TLS certificate verification is disabled for this run.")
        os.environ["YEEDU_CLI_VERIFY_SSL"] = "false"

    client = YeeduClient(dry_run=args.dry_run)

    print("== Step 1/3: Auth ==")
    if args.token:
        configure_auth.inject_token(args.api_url, args.token)
    else:
        configure_auth.login_with_credentials(
            args.api_url, args.username, args.password, dry_run=args.dry_run
        )
    configure_auth.smoke_test(client)

    print("\n== Step 2/3: Tenant ==")
    resolve_tenant_workspace.associate_tenant(client, args.tenant_id)

    print("\n== Step 3/3: Clusters (S/M/L/XL) ==")
    clusters = deploy_clusters.deploy_all(
        client,
        username=args.cluster_username,
        password=args.cluster_password,
        endpoint=args.endpoint,
        onprem_node_names=args.onprem_node_names,
        boot_disk_image=args.boot_disk_image,
        spark_infra_version_id=args.spark_infra_version_id,
    )

    print("\n== Summary ==")
    for c in clusters:
        print(f"  - {c['tier']}: cluster_conf_id={c['cluster_conf_id']} cluster_id={c['cluster_id']} (not started)")


if __name__ == "__main__":
    main()
