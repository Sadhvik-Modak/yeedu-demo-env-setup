#!/usr/bin/env python3
"""Registers the hive_metastore/ Docker stack (Hive Metastore + MinIO S3)
as a Hive metastore-catalog in Yeedu, via `yeedu metastore-catalog hive
create`/`edit`. Idempotent: re-running with the same --name updates the
existing catalog instead of duplicating it.

hive_metastore/conf/hive-site.xml and core-site.xml are docker-compose-
internal only (service DNS names, ${env.VAR} placeholders) — this script
generates a separate, host-facing pair (thrift URI + S3A creds pointed at
--host) in a temp dir, uploads them, and discards them.

Prerequisite: hive_metastore/ must already be up (`docker compose up -d
--build` in that directory) and reachable from wherever the `yeedu`
platform/cluster lives at --host.

Auth pattern and CLI quirks match automation/provision.py — see
automation/README.md. Two gotchas specific to this script, confirmed live
against dev-onprem-008:
  - --api-url must NOT include a path suffix like /api/v1 — the CLI
    appends its own path, and a doubled-up path breaks login with a
    misleading "Auth Token not found" error.
  - Yeedu rejects hyphens in metastore-catalog names (letters/digits/
    underscore only, must start with a letter or underscore).
"""
import argparse
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import configure_auth  # noqa: E402
import resolve_tenant_workspace  # noqa: E402
from yeedu_client import YeeduClient, find_exact_match, get_field, run_allow_not_found  # noqa: E402

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

HIVE_SITE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://{host}:{metastore_port}</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://warehouse/</value>
  </property>
</configuration>
"""

CORE_SITE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://{host}:{minio_port}</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>{minio_access_key}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>{minio_secret_key}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
</configuration>
"""


def parse_env_file(path):
    """Minimal KEY=VALUE parser for hive_metastore/.env (no quoting, # comments)."""
    values = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def validate_name(name):
    if not NAME_RE.match(name):
        raise SystemExit(
            f"Invalid metastore name '{name}': must be 1-64 characters, start "
            "with a letter or underscore, and contain only letters, digits, "
            "or underscores (confirmed live: Yeedu rejects hyphens)."
        )


def render_configs(tmp_dir, host, metastore_port, minio_port, minio_access_key, minio_secret_key):
    hive_site_path = os.path.join(tmp_dir, "hive-site.xml")
    core_site_path = os.path.join(tmp_dir, "core-site.xml")
    with open(hive_site_path, "w") as f:
        f.write(HIVE_SITE_TEMPLATE.format(host=host, metastore_port=metastore_port))
    with open(core_site_path, "w") as f:
        f.write(CORE_SITE_TEMPLATE.format(
            host=host, minio_port=minio_port,
            minio_access_key=minio_access_key, minio_secret_key=minio_secret_key,
        ))
    return hive_site_path, core_site_path


def create_or_update_catalog(client, name, hive_site_path, core_site_path):
    print(f"Checking for an existing metastore-catalog named '{name}'...")
    existing = run_allow_not_found(client, "metastore-catalog", "search", "--metastore_catalog_name", name)
    match = find_exact_match(existing, "name", name)

    if match:
        catalog_id = get_field(match, "metastore_catalog_id")
        print(f"Found existing catalog id={catalog_id} — updating it (`hive edit`)...")
        result = client.run(
            "metastore-catalog", "hive", "edit",
            "--metastore_catalog_id", str(catalog_id),
            "--hive_site_xml_file_path", hive_site_path,
            "--core_site_xml_file_path", core_site_path,
        )
    else:
        print(f"No existing catalog named '{name}' — creating it (`hive create`)...")
        result = client.run(
            "metastore-catalog", "hive", "create",
            "--name", name,
            "--hive_site_xml_file_path", hive_site_path,
            "--core_site_xml_file_path", core_site_path,
        )
    catalog_id = get_field(result, "metastore_catalog_id", default="<dry-run-id>")
    print(f"Metastore-catalog '{name}' ready: metastore_catalog_id={catalog_id}")
    return catalog_id


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--api-url", required=True,
        help="e.g. https://dev-onprem-008.yeedu.io:8080 (no /api/v1 suffix)",
    )
    parser.add_argument("--token", default=None, help="Pre-obtained Yeedu API token")
    parser.add_argument("--username", default=None, help="Alternative to --token: username for `yeedu configure` login")
    parser.add_argument("--password", default=None, help="Alternative to --token: password for `yeedu configure` login")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument(
        "--host", required=True,
        help="LAN/VPN-reachable address of the machine running hive_metastore/ "
             "(not localhost/127.0.0.1, unless Yeedu itself runs on this machine)",
    )
    parser.add_argument(
        "--name", default="docker_hive_metastore",
        help="Metastore-catalog name (letters/digits/underscore only, no hyphens)",
    )
    parser.add_argument("--metastore-port", type=int, default=9083)
    parser.add_argument("--minio-port", type=int, default=9000)
    parser.add_argument(
        "--env-file", default=None,
        help="Path to hive_metastore/.env, for MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
             "(default: <repo_root>/hive_metastore/.env)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print every `yeedu` command without executing anything.")
    parser.add_argument(
        "--insecure", action="store_true",
        help="Set YEEDU_CLI_VERIFY_SSL=false — needed for on-prem/dev hosts with self-signed certs.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_file = args.env_file or os.path.join(repo_root, "hive_metastore", ".env")

    if not args.token and not (args.username and args.password):
        raise SystemExit("Provide either --token, or both --username and --password.")

    validate_name(args.name)

    if not os.path.isfile(env_file):
        raise SystemExit(f"Env file not found: {env_file} (bring hive_metastore/ up first — see its README).")
    env_values = parse_env_file(env_file)
    for required in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"):
        if required not in env_values:
            raise SystemExit(f"{required} missing from {env_file}")

    if args.insecure:
        print("WARNING: --insecure set — TLS certificate verification is disabled for this run.")
        os.environ["YEEDU_CLI_VERIFY_SSL"] = "false"
    # Set once, for the whole process: every `yeedu` subprocess call inherits
    # it. Confirmed live: relying on configure_auth to set this isn't enough
    # if a stale session for a *different* host was cached earlier.
    os.environ["YEEDU_RESTAPI_URL"] = args.api_url

    client = YeeduClient(dry_run=args.dry_run)

    print("== Step 1/3: Auth ==")
    if args.token:
        configure_auth.inject_token(args.api_url, args.token)
    else:
        configure_auth.login_with_credentials(args.api_url, args.username, args.password, dry_run=args.dry_run)
    configure_auth.smoke_test(client)

    print("\n== Step 2/3: Tenant ==")
    resolve_tenant_workspace.associate_tenant(client, args.tenant_id)

    print("\n== Step 3/3: Register Hive metastore-catalog ==")
    with tempfile.TemporaryDirectory() as tmp_dir:
        hive_site_path, core_site_path = render_configs(
            tmp_dir, args.host, args.metastore_port, args.minio_port,
            env_values["MINIO_ROOT_USER"], env_values["MINIO_ROOT_PASSWORD"],
        )
        catalog_id = create_or_update_catalog(client, args.name, hive_site_path, core_site_path)

    print("\n== Summary ==")
    print(f"metastore_catalog_id={catalog_id} name={args.name} host={args.host}")


if __name__ == "__main__":
    main()
