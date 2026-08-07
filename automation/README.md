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
  [--workspace-id <workspace_id>] \
  [--workspace-name <name>] \
  [--cluster-id <cluster_id>] \
  [--git-branch main] \
  [--start] \
  [--skip-notebook-confirm] \
  [--insecure] \
  [--dry-run]
```

Auth: pass **either** `--token <token>`, **or** `--username <u> --password
<p>` (falls back to a real `yeedu configure` login instead of token
injection — use this if you don't have a fresh token handy).

- `--workspace-id` is optional. If omitted, a **new** workspace is created
  every run (name `yeedu-demo-<timestamp>`, override with
  `--workspace-name`) — not idempotent by design, since a fresh workspace
  per run was the explicit ask. Pass an existing `--workspace-id` to reuse
  one instead (jobs/notebooks stay idempotent either way).
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

## Confirmed live (2026-08-07, dev-onprem-008, tenant 3337654a-ec94-4f4f-9eac-5907d8dae9ed)

A full real run succeeded end to end: auth (both token-injection and
username/password), tenant associate, workspace auto-create (`workspace_id
957`), repo clone, all 3 Functions jobs (`job_id` 4215/4216/4217), and all
17 notebooks (`notebook_id` 4218–4234) — including manually confirming in
the Yeedu UI that the first notebook created showed real cloned cell
content, not an empty notebook. **Known gaps #1 and #2 below are now
resolved**, kept here as a record of what was actually verified vs.
assumed going in:

- **Token-injection via `yeedu.yml` works** — the CLI picked up and used an
  injected token correctly with 3 environments configured, no
  ambiguity/wrong-env issue. Username/password via `yeedu configure` also
  confirmed working (`configure_auth.login_with_credentials`), for when a
  fresh token isn't in hand.
- **`notebook create --notebook_path <path>` does adopt the existing
  cloned `.ipynb` file** — confirmed by manual UI check on notebook 4218
  before batch-creating the rest.
- Three real bugs found and fixed during this run:
  1. `YeeduClient.run()` treated exit code 0 as success even when the
     response *body* was an API-level error (e.g. an expired token still
     exits 0 with `{'error_code': 'RFA-000001', ...}`). Fixed: body is
     inspected for `error_code`/`error_message` regardless of exit code.
  2. Despite the flag name, `--json-output default` doesn't always emit
     real JSON — error bodies came back as **Python dict repr**
     (single-quoted keys), which `json.loads` rejects. Fixed: falls back
     to `ast.literal_eval`.
  3. `job search` / `notebook search` for a name that doesn't exist yet
     (the idempotency check) also exits 0 with an error-shaped body — and
     the wording isn't consistent (`"...is not found within..."` for
     jobs, `"No notebook matches were found for..."` for notebooks).
     Fixed: `yeedu_client.run_allow_not_found()` catches both phrasings
     and returns `None` instead of raising, so the idempotency check can
     proceed to create the resource.
- `YEEDU_CLI_VERIFY_SSL=false` is required against this host (self-signed
  cert) — exposed as the explicit `--insecure` flag, per the skill's own
  TLS-weakening-needs-sign-off gotcha, rather than a silent default.
- Also hit the skill's documented gotcha #2 live: this machine's
  `~/.bashrc` already exports a *different* default `YEEDU_RESTAPI_URL`
  (`dev-onprem-005`) — confirms the script must never rely on ambient env
  vars for the target host, only explicit `--api-url`/`--username`, which
  `provision.py` already does correctly.
- The new `--json-output`/exit-code/not-found findings were written back
  into `~/.claude/skills/yeedu-cli/reference/known-issues-and-gotchas.md`
  as gotcha #11 for future sessions.

## Known gaps — read before a real run

1. **Git-clone and tenant-associate REST shapes** are sourced only from the
   `yeedu-cli` skill's CLI-level docs (2.10.1-live-verified per its own
   header), not from an OpenAPI spec — consistent with everything else
   this script does, but worth knowing if something doesn't match.
2. **Clone response `file_id` extraction is unconfirmed** — `clone_repo.py`
   couldn't find a `file_id`/`workspace_file_id`/`id` key in the real
   `git clone` response during the live run (clone itself succeeded; only
   the returned identifier used for the "already cloned, pull instead"
   idempotency path is affected — that path was tested successfully via
   `workspace list-workspace-files` name-matching instead, so this isn't
   currently blocking, just an unconfirmed field name).

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
| `yeedu_client.py` | Subprocess wrapper: `YeeduClient.run(*args)` → parsed JSON, appends `--json-output default`, handles the exit-0-with-error-body and Python-repr quirks. `run_allow_not_found()` wraps idempotency-check calls, tolerating both "not found" wordings seen live. |
| `configure_auth.py` | `inject_token()` (writes `~/.yeedu/yeedu.yml`) or `login_with_credentials()` (`yeedu configure` with username/password) — either path, then an auth smoke test. |
| `resolve_tenant_workspace.py` | `iam associate-tenant`, `workspace get`, and `create_workspace()` (auto-creates a new workspace per run when `--workspace-id` is omitted). |
| `clone_repo.py` | Idempotent git clone/pull; also defines `REPO_WORKSPACE_PATH`, the in-workspace root (`/files/yeedu-demo-env-setup`) that `deploy_functions.py`/`create_notebooks.py` build paths from. |
| `deploy_functions.py` | Creates the 3 Functions jobs; strips version pins from `requirements.txt` per gotcha #4 (`--yeedu-functions-requirements` does a raw space-split, not JSON/comma parsing). |
| `create_notebooks.py` | Discovers `notebooks/**/*.ipynb`, creates each as a notebook; pauses after the first for a manual UI check unless `--skip-notebook-confirm`. |

## Verify without a live instance

```bash
python3 -m py_compile automation/*.py
python3 automation/provision.py --dry-run --api-url https://x --token x --tenant-id x --workspace-id x
```
