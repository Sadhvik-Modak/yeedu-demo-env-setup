"""Creates S/M/L/XL cluster tiers on an OnPrem Yeedu environment —
config only, never starts them. Mirrors the reference cluster `onprem-xs`
(cluster_id 357) found on dev-onprem-005, tenant
7e27253a-1ab8-4623-a789-56ae7e8939e9: same network bridge/model, boot disk
image, and OnPrem endpoint/node names, scaled across 4 machine-type tiers.

Password is NOT retrievable from any existing credential config (Proxmox
Basic Auth's password field is write-only, confirmed live: `get-
credential-conf` echoes back `{"credentials": {"USERNAME": "..."}}`, no
PASSWORD key ever). This module always creates its OWN credential config
with a caller-supplied username/password — defaults to an obvious dummy
placeholder per instruction ("use dummy password for now"). Replace with
a real one (`resource edit-credential-conf` — password is not exposed via
this automation, only creatable) before actually starting any cluster
built from it.

CONFIRMED LIVE (2026-08-07) against dev-onprem-005:
- `resource create-credential-conf --base64_encoded_credentials` expects
  base64(json.dumps({"USERNAME": ..., "PASSWORD": ...})) for
  credential_type_id 3 (Proxmox Basic Auth) — verified round-trip (created,
  then `get-credential-conf` echoed USERNAME back correctly).
- `cluster create-conf` takes raw disk params (`--disk_type_id --size
  --number_of_disks`), NOT a volume_conf_id, despite `cluster get`
  displaying the resulting disk as a nested `machine_volume_conf` object —
  the platform creates/matches one internally.
- `cluster create --cluster_type YEEDU` REQUIRES `--min_instances` and
  `--max_instances` despite both being shown as optional in `--help`:
  `{'error_code': 'RFA-000044', 'error_message': "The parameters
  'min_instances' and 'max_instances' are required for the 'YEEDU' and
  'STANDALONE' cluster types."}`.
- A freshly created, never-started cluster's `cluster_status` (and its one
  node's `node_status`) is `DESTROYED` — this is the normal "no compute
  provisioned yet" rest state, not a failure. Confirmed via `cluster get`
  immediately after creation, no errors anywhere in the chain.
- Create-response field names are inconsistent per resource type — e.g.
  `create-network-conf` returns the name under `name`, but
  `create-boot-disk-image-conf` returns it under
  `boot_disk_image_name` (matching its own `get`/`search` field name).
  This module only ever extracts IDs from create responses, never names,
  to sidestep the inconsistency.
"""
import base64
import json

from yeedu_client import find_exact_match, get_field, run_allow_not_found

CLOUD_PROVIDER_ID_ONPREM = 3
CREDENTIAL_TYPE_ID_PROXMOX_BASIC_AUTH = 3
DISK_TYPE_ID_LOCAL_LVM = 10
LINUX_DISTRO_ID_UBUNTU_22_04 = 1
ARCHITECTURE_TYPE_ID_X86_64 = 0

# Defaults taken from the reference cluster (onprem-xs, cluster_id 357,
# cloud_env_id 63 "sc2302_onprem") on dev-onprem-005 — override for a
# different OnPrem environment.
DEFAULT_ENDPOINT = "https://10.50.0.121:8006/"
DEFAULT_ONPREM_NODE_NAMES = "pve1,pve2,pve3,pve4,pve5"
DEFAULT_NETWORK_BRIDGE = "vmbr0"
DEFAULT_NETWORK_MODEL = "e1000e"
DEFAULT_BOOT_DISK_IMAGE = "jammy-server-cloudimg-amd64.qcow2"
DEFAULT_SPARK_INFRA_VERSION_ID = 5  # Spark 3.5.3 / Hadoop 3.2.4 / Scala 2.12.15
DEFAULT_USERNAME = "yeedu-demo@pam"
DEFAULT_PASSWORD = "CHANGEME-dummy-password"  # noqa: S105 — intentional placeholder, not a real secret

RESOURCE_NAME_PREFIX = "yeedu-demo-env-setup"

# machine_type_id per tier — confirmed live via
# `resource list-provider-machine-types --cloud_provider_id 3 --all true`.
# disk_size_gb matches this environment's existing volume_onprem_<tier>
# confs (200/200/400/500 GB) for consistency, though this module creates
# its own disk config via raw params rather than referencing those.
TIERS = [
    {"tier": "S", "machine_type_id": 1486, "machine_type_name": "Onprem-S-4 (4 vCPU / 16GB)", "disk_size_gb": 200},
    {"tier": "M", "machine_type_id": 1487, "machine_type_name": "Onprem-M-8 (8 vCPU / 16GB)", "disk_size_gb": 200},
    {"tier": "L", "machine_type_id": 1488, "machine_type_name": "Onprem-L-16 (16 vCPU / 32GB)", "disk_size_gb": 400},
    {"tier": "XL", "machine_type_id": 1489, "machine_type_name": "Onprem-XL-32 (32 vCPU / 64GB)", "disk_size_gb": 500},
]


def _find_by_name(client, list_args, name_field, name_value):
    result = run_allow_not_found(client, *list_args)
    return find_exact_match(result, name_field, name_value)


def ensure_network_conf(client):
    name = f"{RESOURCE_NAME_PREFIX}_network"
    existing = _find_by_name(
        client, ["resource", "search-network-confs", "--network_conf_name", name],
        "network_conf_name", name,
    )
    if existing:
        print(f"  network-conf already exists (id={existing['network_conf_id']}) — reusing.")
        return existing["network_conf_id"]
    result = client.run(
        "resource", "create-network-conf",
        "--name", name,
        "--cloud_provider_id", str(CLOUD_PROVIDER_ID_ONPREM),
        "--bridge", DEFAULT_NETWORK_BRIDGE,
        "--model", DEFAULT_NETWORK_MODEL,
    )
    network_conf_id = get_field(result, "network_conf_id", default="<dry-run-network-conf-id>")
    print(f"  created network-conf id={network_conf_id}")
    return network_conf_id


def ensure_boot_disk_image_conf(client, boot_disk_image=DEFAULT_BOOT_DISK_IMAGE):
    name = f"{RESOURCE_NAME_PREFIX}_bootdisk"
    existing = _find_by_name(
        client, ["resource", "search-boot-disk-image-confs", "--boot_disk_image_name", name],
        "boot_disk_image_name", name,
    )
    if existing:
        print(f"  boot-disk-image-conf already exists (id={existing['boot_disk_image_id']}) — reusing.")
        return existing["boot_disk_image_id"]
    result = client.run(
        "resource", "create-boot-disk-image-conf",
        "--name", name,
        "--cloud_provider_id", str(CLOUD_PROVIDER_ID_ONPREM),
        "--linux_distro_id", str(LINUX_DISTRO_ID_UBUNTU_22_04),
        "--boot_disk_image", boot_disk_image,
        "--architecture_type_id", str(ARCHITECTURE_TYPE_ID_X86_64),
    )
    boot_disk_image_id = get_field(result, "boot_disk_image_id", default="<dry-run-boot-disk-image-id>")
    print(f"  created boot-disk-image-conf id={boot_disk_image_id}")
    return boot_disk_image_id


def ensure_credential_conf(client, username, password):
    name = f"{RESOURCE_NAME_PREFIX}_cred"
    existing = _find_by_name(
        client, ["resource", "search-credential-confs", "--credentials_conf_name", name],
        "credentials_conf_name", name,
    )
    if existing:
        print(
            f"  credential-conf already exists (id={existing['credentials_conf_id']}) "
            "— reusing (password NOT updated; use `resource edit-credential-conf` to change it)."
        )
        return existing["credentials_conf_id"]
    creds_b64 = base64.b64encode(
        json.dumps({"USERNAME": username, "PASSWORD": password}).encode()
    ).decode()
    result = client.run(
        "resource", "create-credential-conf",
        "--name", name,
        "--credential_type_id", str(CREDENTIAL_TYPE_ID_PROXMOX_BASIC_AUTH),
        "--base64_encoded_credentials", creds_b64,
    )
    credentials_conf_id = get_field(result, "credentials_conf_id", default="<dry-run-credential-conf-id>")
    print(f"  created credential-conf id={credentials_conf_id} (username={username!r}, password is a placeholder)")
    return credentials_conf_id


def ensure_cloud_env(client, network_conf_id, credential_config_id, boot_disk_image_id, endpoint, onprem_node_names):
    name = f"{RESOURCE_NAME_PREFIX}_cloud_env"
    existing = _find_by_name(
        client, ["resource", "search-cloud-envs", "--cloud_env_name", name],
        "cloud_env_name", name,
    )
    if existing:
        print(f"  cloud-env already exists (id={existing['cloud_env_id']}) — reusing.")
        return existing["cloud_env_id"]
    result = client.run(
        "resource", "create-cloud-env",
        "--name", name,
        "--cloud_provider_id", str(CLOUD_PROVIDER_ID_ONPREM),
        "--network_conf_id", str(network_conf_id),
        "--credential_config_id", str(credential_config_id),
        "--boot_disk_image_id", str(boot_disk_image_id),
        "--endpoint", endpoint,
        "--onprem_node_names", onprem_node_names,
    )
    cloud_env_id = get_field(result, "cloud_env_id", default="<dry-run-cloud-env-id>")
    print(f"  created cloud-env id={cloud_env_id}")
    return cloud_env_id


def ensure_cluster_conf(client, tier):
    name = f"{RESOURCE_NAME_PREFIX}-{tier['tier'].lower()}"
    existing = _find_by_name(
        client, ["cluster", "search-confs", "--cluster_conf_name", name],
        "cluster_conf_name", name,
    )
    if existing:
        print(f"  cluster-conf already exists (id={existing['cluster_conf_id']}) — reusing.")
        return existing["cluster_conf_id"]
    result = client.run(
        "cluster", "create-conf",
        "--name", name,
        "--machine_type_id", str(tier["machine_type_id"]),
        "--disk_type_id", str(DISK_TYPE_ID_LOCAL_LVM),
        "--size", str(tier["disk_size_gb"]),
        "--number_of_disks", "1",
    )
    cluster_conf_id = get_field(result, "cluster_conf_id", default="<dry-run-cluster-conf-id>")
    print(f"  created cluster-conf id={cluster_conf_id}")
    return cluster_conf_id


def ensure_cluster(client, tier, cluster_conf_id, cloud_env_id, spark_infra_version_id):
    name = f"{RESOURCE_NAME_PREFIX}-{tier['tier'].lower()}"
    existing = _find_by_name(client, ["cluster", "search", "--cluster_name", name], "name", name)
    if existing:
        print(f"  cluster already exists (id={existing['cluster_id']}, status={existing.get('cluster_status')}) — reusing.")
        return existing["cluster_id"]
    result = client.run(
        "cluster", "create",
        "--name", name,
        "--cloud_env_id", str(cloud_env_id),
        "--spark_infra_version_id", str(spark_infra_version_id),
        "--cluster_type", "YEEDU",
        "--cluster_conf_id", str(cluster_conf_id),
        # Required for YEEDU/STANDALONE cluster types despite --help
        # showing both as optional — see module docstring.
        "--min_instances", "1",
        "--max_instances", "1",
    )
    cluster_id = get_field(result, "cluster_id", default="<dry-run-cluster-id>")
    print(f"  created cluster id={cluster_id} (not started)")
    return cluster_id


def deploy_all(
    client,
    username=DEFAULT_USERNAME,
    password=DEFAULT_PASSWORD,
    endpoint=DEFAULT_ENDPOINT,
    onprem_node_names=DEFAULT_ONPREM_NODE_NAMES,
    boot_disk_image=DEFAULT_BOOT_DISK_IMAGE,
    spark_infra_version_id=DEFAULT_SPARK_INFRA_VERSION_ID,
):
    print("== Cluster infra: network / boot disk / credential / cloud env ==")
    network_conf_id = ensure_network_conf(client)
    boot_disk_image_id = ensure_boot_disk_image_conf(client, boot_disk_image=boot_disk_image)
    credential_config_id = ensure_credential_conf(client, username, password)
    cloud_env_id = ensure_cloud_env(
        client, network_conf_id, credential_config_id, boot_disk_image_id, endpoint, onprem_node_names
    )

    created = []
    for tier in TIERS:
        print(f"\n== Cluster tier {tier['tier']} ({tier['machine_type_name']}) ==")
        cluster_conf_id = ensure_cluster_conf(client, tier)
        cluster_id = ensure_cluster(client, tier, cluster_conf_id, cloud_env_id, spark_infra_version_id)
        created.append({"tier": tier["tier"], "cluster_conf_id": cluster_conf_id, "cluster_id": cluster_id})
    return created
