# Yeedu provisioning automation

Provisions a live Yeedu workspace from this repo, end to end:

1. Clones [`Sadhvik-Modak/yeedu-demo-env-setup`](https://github.com/Sadhvik-Modak/yeedu-demo-env-setup)
   into the target workspace (idempotent — pulls instead of re-cloning if
   already present).
2. Registers each `functions/*` demo as a Yeedu Functions job (idempotent —
   reuses an existing job by name instead of duplicating).
3. Registers each `notebooks/**/*.ipynb` as a Yeedu notebook, with
   `notebook_type` inferred from the filename (`*_sql.ipynb` → `sql`,
   everything else → `python3`) (idempotent, same as above).

Targets **Yeedu platform 2.10.1**, driven via the `yeedu` CLI (package
`yeedu-cli`) rather than raw HTTP — see "Why the CLI, not raw REST" below.

## Install

```bash
pip install -r automation/requirements.txt
```

## Usage

```bash
python3 automation/provision.py \
  --api-url https://<host>:8080 \
  --token <pre-obtained yeedu api token> \
  --tenant-id <tenant_id> \
  --workspace-id <workspace_id> \
  [--cluster-id <cluster_id>] \
  [--git-branch main] \
  [--start] \
  [--skip-notebook-confirm] \
  [--dry-run]
```

- `--cluster-id` is optional. Without it, jobs/notebooks are created but
  **not** started (a cluster is required to actually run anything) —
  matches "run isn't mandatory this pass."
- `--start` additionally starts each Functions job after creating it
  (requires `--cluster-id`; ignored otherwise, with a warning). Notebooks
  are never auto-started by this script — start them from the Yeedu UI or
  `yeedu notebook start` once you've confirmed they look right.
- `--dry-run` prints every `yeedu` command this script would run, without
  executing anything — run this first against your real values to sanity
  check the command construction before touching a live workspace.
- `--insecure` sets `YEEDU_CLI_VERIFY_SSL=false` — needed for on-prem/dev
  hosts with self-signed certs (confirmed live: without it, `yeedu` exits
  255 asking for `YEEDU_SSL_CERT_FILE`). Weakens TLS verification; only use
  it against a known dev/QA sandbox.
- Run `--dry-run` at least once before a real run.

## Confirmed live (2026-08-07, against dev-onprem-008)

- The `yeedu.yml` token-injection mechanism (Known gap #1) **does work** —
  the CLI picked up and used the injected token correctly with 3
  environments configured (no ambiguity/wrong-env issue hit). Full
  success-path response shape is still unconfirmed (the token used for
  this test had expired), but the injection + selection mechanism itself
  is no longer a guess.
- Found and fixed two real bugs this surfaced:
  - `YeeduClient.run()` was treating exit code 0 as success even when the
    response *body* was an API-level error — `yeedu iam get-user-info`
    with an expired token exits 0 with
    `{'error_code': 'RFA-000001', 'error_message': 'User session has
    expired. Please login again.'}`. Fixed: the body is now inspected for
    `error_code`/`error_message` regardless of exit code.
  - Despite the flag name, `--json-output default` does not always emit
    real JSON — that error response above is **Python dict repr**
    (single-quoted keys), which `json.loads` rejects. Fixed: falls back to
    `ast.literal_eval` before giving up and treating output as opaque text.
- `YEEDU_CLI_VERIFY_SSL=false` is required against this host (self-signed
  cert) — added as an explicit `--insecure` flag rather than a silent
  default, per the skill's own TLS-weakening-needs-sign-off gotcha.
- Also hit the skill's documented gotcha #2 live: this machine's
  `~/.bashrc` already exports a *third*, different `YEEDU_RESTAPI_URL`
  (`dev-onprem-005`) — confirms the script must never rely on ambient
  env vars for the target host, only explicit `--api-url`. `provision.py`
  already does this correctly (via `configure_auth.inject_token`, not by
  reading `YEEDU_RESTAPI_URL` from the shell).

## Known gaps — read before a real run

1. **Full auth success-path response shape** — the token used above was
   expired, so a *successful* `iam get-user-info` response was never
   observed live, only the error shape. Re-run the smoke test
   (`configure_auth.smoke_test`) with a fresh token to confirm.
2. **`notebook create --notebook_path <path>` semantics are unverified.**
   This script assumes it adopts an existing `.ipynb` file already present
   at that path in the workspace (since the repo is git-cloned in first) —
   that's the only reading that makes sense functionally, but no
   confirmed schema or example says so either way. To de-risk: the script
   creates the *first* notebook alone, prints its workspace path, and
   pauses (`input()`) for you to check the Yeedu UI before batch-creating
   the rest. Pass `--skip-notebook-confirm` once you've verified this once
   and trust it. **If the created notebook comes back empty** instead of
   showing the real bronze/gold/SQL cells, the fix is almost certainly to
   push content via `yeedu workspace create-workspace-file` instead of (or
   in addition to) `notebook create --notebook_path`, and
   `create_notebooks.py` will need a follow-up patch — file location:
   `automation/create_notebooks.py:_create_one`.
3. **Git-clone and tenant-associate REST shapes** are sourced only from the
   `yeedu-cli` skill's CLI-level docs (2.10.1-live-verified per its own
   header), not from an OpenAPI spec — consistent with everything else
   this script does, but worth knowing if something doesn't match.

## Why the CLI, not raw REST

- The live swagger UI (`{api_url}/api-docs/`) returns only its HTML shell;
  the actual spec (`/v2/api-docs` / `/v3/api-docs`) returned
  `401 "Auth Token not found"` with no token available to authenticate and
  pull it during development.
- The `yeedu-docs` repo's checked-in OpenAPI spec only goes up to v2.9.0
  (confirms job/notebook config schemas, but has zero git-clone endpoint —
  grepped exhaustively across every version).
- `~/.claude/skills/yeedu-cli/` is a skill written specifically against
  **CLI/platform 2.10.1**, sourced from live `--help` output (not a stale
  source checkout), including 10 documented operational gotchas. This is
  the most trustworthy 2.10.1-matched source available, hence: drive the
  `yeedu` CLI via subprocess rather than hand-roll unconfirmed raw HTTP.

## Files

| File | Purpose |
|---|---|
| `provision.py` | Entrypoint — argparse, runs steps 1-5 in order, prints a summary. |
| `yeedu_client.py` | Subprocess wrapper: `YeeduClient.run(*args)` → parsed JSON, always appends `--json-output default`. |
| `configure_auth.py` | Token injection + auth smoke test (Known gap #1). |
| `resolve_tenant_workspace.py` | `iam associate-tenant`, `workspace get`. |
| `clone_repo.py` | Idempotent git clone/pull; also defines `REPO_WORKSPACE_PATH`, the assumed in-workspace root (`/files/yeedu-demo-env-setup`) that `deploy_functions.py`/`create_notebooks.py` build paths from. |
| `deploy_functions.py` | Creates the 3 Functions jobs; strips version pins from `requirements.txt` per gotcha #4 (`--yeedu-functions-requirements` does a raw space-split, not JSON/comma parsing). |
| `create_notebooks.py` | Discovers `notebooks/**/*.ipynb`, creates each as a notebook (Known gap #2). |

## Verify without a live instance

```bash
python3 -m py_compile automation/*.py
python3 automation/provision.py --dry-run --api-url https://x --token x --tenant-id x --workspace-id x
```
