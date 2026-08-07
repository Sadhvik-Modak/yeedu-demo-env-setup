# Yeedu Cluster Tiers (S/M/L/XL)

Creates 4 Yeedu clusters — S, M, L, XL — on an OnPrem environment, scaled
across machine types. **Config only — never starts them** (a Yeedu cluster
with no active compute costs nothing and shows `cluster_status:
DESTROYED`, the normal rest state, until `cluster start` is called).

Mirrors the reference cluster `onprem-xs` (`cluster_id` 357) found on
`dev-onprem-005`, tenant `7e27253a-1ab8-4623-a789-56ae7e8939e9`: same
network bridge/model (`vmbr0`/`e1000e`), boot disk image
(`jammy-server-cloudimg-amd64.qcow2`, Ubuntu 22.04), and OnPrem
endpoint/node names — see `automation/deploy_clusters.py` for the exact
defaults.

## Tiers

| Tier | Machine type | vCPU | Memory | Disk |
|---|---|---|---|---|
| S | Onprem-S-4 | 4 | 16GB | 200GB |
| M | Onprem-M-8 | 8 | 16GB | 200GB |
| L | Onprem-L-16 | 16 | 32GB | 400GB |
| XL | Onprem-XL-32 | 32 | 64GB | 500GB |

(Machine types were already provisioned on this environment — confirmed
live via `resource list-provider-machine-types --cloud_provider_id 3`.)

## Password

The reference cluster's credential (Proxmox Basic Auth) has a write-only
password field — not retrievable via the API. This automation always
creates its **own** credential config with a placeholder dummy password
(`--cluster-password`, defaults to an obvious `CHANGEME-...` value) rather
than depending on any pre-existing credential. Replace it with a real
password (`resource edit-credential-conf`) before starting any cluster
built from it.

## Deploying

```bash
python3 automation/create_clusters.py \
  --api-url https://<onprem-host>:8080 \
  --username <user> --password <pass> \
  --tenant-id <tenant_id> \
  --cluster-password <real password, or leave as the dummy default> \
  --insecure
```

Idempotent throughout — creates network/boot-disk/credential/cloud-env
config and all 4 cluster-confs + clusters, or finds and reuses each by
name if it already exists. See `automation/deploy_clusters.py`'s
docstring for confirmed-live gotchas (required-but-undocumented
`min_instances`/`max_instances`, the credential JSON shape, etc).

## Live resources (dev-onprem-005, tenant `7e27253a-1ab8-4623-a789-56ae7e8939e9`)

Created 2026-08-07, all currently `DESTROYED` (not started):

| Resource | ID |
|---|---|
| network-conf (`yeedu-demo-env-setup_network`) | 69 |
| boot-disk-image-conf (`yeedu-demo-env-setup_bootdisk`) | 30 |
| credential-conf (`yeedu-demo-env-setup_cred`, **dummy password**) | 51 |
| cloud-env (`yeedu-demo-env-setup_cloud_env`) | 64 |
| cluster-conf S / M / L / XL | 650 / 651 / 652 / 653 |
| cluster S / M / L / XL | 359 / 360 / 361 / 362 |

## Starting a cluster

Set up the shell once:

```bash
export YEEDU_CLI_VERIFY_SSL=false
export YEEDU_RESTAPI_URL='https://dev-onprem-005.yeedu.io:8080/'
echo '{"token": "<your-token>"}' > ~/.yeedu/yeedu_cli.config
yeedu iam associate-tenant --tenant_id 7e27253a-1ab8-4623-a789-56ae7e8939e9
```

Replace the dummy password first (credential 51) or `cluster start` will
fail against the real Proxmox host:

```bash
CREDS_B64=$(echo -n '{"USERNAME": "yeedu-demo@pam", "PASSWORD": "<real-password>"}' | base64 -w0)
yeedu resource edit-credential-conf --credentials_conf_id 51 --base64_encoded_credentials "$CREDS_B64"
```

Then start whichever tier(s):

```bash
yeedu cluster start --cluster_id 359   # S
yeedu cluster start --cluster_id 360   # M
yeedu cluster start --cluster_id 361   # L
yeedu cluster start --cluster_id 362   # XL
```

Returns immediately (async, like `git clone` — see `automation/README.md`
"Confirmed live"); poll with `yeedu cluster get --cluster_id <id>
--json-output default` and check `cluster_status`.

Once `RUNNING`, to also start the demo jobs/notebooks against it, re-run
`automation/provision.py` with `--cluster-id <id> --start --workspace-id
959` — attaches the cluster and starts each Functions/JAR/Python/SQL job.

## Out of scope (for now)

Metastore and dependency-repository (object storage manager) config —
left for a later pass.
