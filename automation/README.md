# Yeedu provisioning automation

Provisions a live Yeedu workspace from this repo, end to end:

1. Clones [`Sadhvik-Modak/yeedu-demo-env-setup`](https://github.com/Sadhvik-Modak/yeedu-demo-env-setup)
   into the target workspace (async — polls `git clone-status` to
   completion; idempotent — pulls instead of re-cloning if a real
   git-tracked folder already exists).
2. Registers each `functions/*` demo as a Yeedu Functions job, and each
   `jobs/{jar,python,sql}` demo as its respective job type (idempotent —
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
- `--start` additionally starts each Functions/JAR/Python/SQL job after
  creating it (requires `--cluster-id`; ignored otherwise, with a
  warning). Notebooks are never auto-started by this script — start them
  from the Yeedu UI or `yeedu notebook start` once you've confirmed they
  look right.
- `--dry-run` prints every `yeedu` command this script would run, without
  executing anything — run this first against your real values to sanity
  check the command construction before touching a live workspace.
- `--insecure` sets `YEEDU_CLI_VERIFY_SSL=false` — needed for on-prem/dev
  hosts with self-signed certs (confirmed live: without it, `yeedu` exits
  255 asking for `YEEDU_SSL_CERT_FILE`). Weakens TLS verification; only use
  it against a known dev/QA sandbox.
- Run `--dry-run` at least once before a real run.

## Confirmed live (2026-08-07, dev-onprem-008, tenant 3337654a-ec94-4f4f-9eac-5907d8dae9ed)

A full real run succeeded end to end and was fully repaired after two
content bugs were caught by the user checking the actual Yeedu UI (not by
this script — see below): auth (both token-injection and
username/password), tenant associate, workspace auto-create (`workspace_id
957`), repo clone, all 3 Functions jobs (`job_id` 4215/4216/4217), and all
17 notebooks (`notebook_id` 4218–4234) with **verified-correct real cell
content** in each. **Known gaps #1 and #2 below are now resolved**, kept
here as a record of what was actually verified vs. assumed going in:

- **Token-injection via `yeedu.yml` works** — the CLI picked up and used an
  injected token correctly with 3 environments configured, no
  ambiguity/wrong-env issue. Username/password via `yeedu configure` also
  confirmed working (`configure_auth.login_with_credentials`), for when a
  fresh token isn't in hand.
- **`notebook create --notebook_path <path>` does NOT adopt existing file
  content** — this was the original hypothesis and it was **wrong**. It
  overwrites whatever's at that path with a blank one-cell skeleton (a
  4054-byte real notebook became 742 bytes), even though the file existed
  with real cloned content already. All 17 notebooks were silently
  blanked out on the first run. Confirmed by downloading and diffing
  notebook 4218's actual stored content. Fixed: `create_notebooks.py` now
  always follows `notebook create` (or reuse) with `workspace
  create-workspace-file --overwrite true`, which preserves the
  notebook↔file_id binding while replacing the bytes with the real repo
  content — verified byte-identical after the fix, and now runs on every
  notebook every time (self-healing, not just on first create).
- **`job search`/`notebook search` returns prefix matches, not just exact
  ones** — searching `gold_citibike_station_activity_v1` also returned
  `gold_citibike_station_activity_v1_sql` in the result list (with the
  wrong one first), which silently returned the wrong `notebook_id` for
  every `<name>`/`<name>_sql` pair in this repo (no data was corrupted —
  the content push is path-based, not id-based — but the id returned/
  logged was wrong, which would matter for any later id-specific call).
  Fixed: `yeedu_client.find_exact_match()` filters search results for an
  exact name match instead of trusting result order; used in both
  `deploy_functions._find_job_id` and `create_notebooks._find_notebook_id`.
- Five real bugs found and fixed total during live testing (see git log
  for the earlier three: exit-0-with-error-body responses, `--json-output
  default` emitting Python repr instead of JSON, and inconsistent
  "not found" wording across search endpoints).
- `YEEDU_CLI_VERIFY_SSL=false` is required against this host (self-signed
  cert) — exposed as the explicit `--insecure` flag, per the skill's own
  TLS-weakening-needs-sign-off gotcha, rather than a silent default.
- Also hit the skill's documented gotcha #2 live: this machine's
  `~/.bashrc` already exports a *different* default `YEEDU_RESTAPI_URL`
  (`dev-onprem-005`) — confirms the script must never rely on ambient env
  vars for the target host, only explicit `--api-url`/`--username`, which
  `provision.py` already does correctly.
- These findings were written back into
  `~/.claude/skills/yeedu-cli/reference/known-issues-and-gotchas.md` as
  gotcha #11 for future sessions.

**Follow-up session, same day:** added `jobs/{jar,python,sql}` (the
remaining job types beyond Functions) and confirmed their `job create`
payload shapes live:

- `job_type: JAR` — CLI enum value is `JAR` (all caps), not `Jar` as shown
  in Yeedu's own OpenAPI example. `job_command` = workspace path to the
  jar + `job_class_name` + `job_arguments`. First tested by pointing at
  Yeedu's internally-vendored `spark-examples_2.12-3.2.2.jar` (`job_id`
  4236, `file:///yeedu/object-storage-manager/...`) — confirmed working,
  but then **switched to committing our own thin jar into the repo
  instead** (`jobs/jar/gold-table-summary-job-1.0.jar`, 3.2 KB, `provided`
  Spark deps) so the JAR demo follows the same "cloned in like everything
  else" pattern as Python/SQL/notebooks, rather than depending on
  whatever a given Yeedu instance happens to have vendored internally.
  Re-confirmed working with the new path/class
  (`io.yeedu.demo.GoldTableSummaryJob`, `job_id` 4266 on workspace 959).
- `job_type: Python` — same shape as JAR: `job_command` = workspace path
  to the `.py` file, `job_arguments` = CLI args. Confirmed (`job_id` 4237).
- `job_type: Spark SQL` — **different from both**: rejects `job_command`
  outright ("Please provide 'job_rawScalaCode' for Spark job of job type
  'Spark SQL'"). The query goes in `job_rawScalaCode`, and
  `--job-raw-scala-code` takes a **local filesystem path** (read
  client-side by the `yeedu` CLI itself and uploaded — confirmed by a
  second rejection when the raw SQL text was passed directly: "The file
  cannot be found at '\<the SQL text\>' for the argument
  --job_raw_scala_code"). Confirmed correct via `yeedu job get` — stored
  `job_rawScalaCode` matches `jobs/sql/gold_table_summary.sql` exactly
  (`job_id` 4238).
- These three findings were added to the skill as gotcha #13.

**Third session, same day — critical bug found by re-validating in a fresh
workspace:** `functions/`, `jobs/`, `automation/`, and the root files had
**never actually been cloned into any workspace** (957 or 958, both now
considered broken/invalid — do not reuse them). Only `notebooks/` existed
in either, and only because `create_notebooks.py`'s own
`create-workspace-file` calls create that directory path as a side
effect — completely independent of `git clone`. Every prior "confirmed"
job (Functions ×3, JAR, Python, SQL) had been pointing at files that were
never actually present; `job create`/`job get` don't validate file
existence, so nothing caught it until manually listing workspace files.

Root cause, two compounding bugs:
1. **`git clone` silently requires `--file_id` or `--file_path`** — the
   yeedu-cli skill's docs list both as optional; they're not. Without one,
   the API returns exit 0 with `{'message': 'Please provide any one of
   file_id or file_path to clone a repo into a workspace.'}` — no
   `error_code`/`error_message`/`error` key, so it slipped straight past
   `YeeduClient`'s error detection and was treated as success.
2. **The "already cloned" idempotency check used the wrong key name**
   (`file_id` instead of the actual `workspace_file_id`), so even if a
   real git-tracked folder had existed, its id would've silently resolved
   to `None`.

Also confirmed while fixing this: **`git clone` is asynchronous** — the
real response is `{'message': 'Git clone repo has been initiated.',
'workspace_file_id': ...}`, and `git clone-status --file_id <id>` needs
polling (took ~60s in testing, `job_status: CLONE_STARTED` →
`COMPLETED`) before the files actually exist.

Fixed: `clone_repo.py` now passes `--file_path /`, extracts
`workspace_file_id` correctly, and polls `clone-status` to completion
(raising on failure/timeout) before returning — callers can now actually
trust the repo is present. The idempotency check also now requires
`is_git: true` on a name-matched folder, so a contaminated non-git folder
(like the ones left in 957/958) can't be mistaken for a real prior clone.
Also broadened the "not found" detector from a literal-phrase list to a
regex (`_NOT_FOUND_RE`) after finding a *fourth* distinct phrasing
(`"No workspace files found for..."` on an empty/new workspace) that the
old list missed too.

**Re-validated end to end in a brand-new workspace 959** with all fixes
applied: clone genuinely completes (`functions/`, `jobs/`, `automation/`,
`README.md` all verified present via `workspace get-workspace-file`,
`is_git: true`, correct byte sizes matching the local repo — e.g. the jar
at exactly 3273 bytes), all 6 jobs (`job_id` 4263–4268), all 17 notebooks
(`notebook_id` 4269–4285). This is the first workspace that's actually
fully correct.

**Fourth session, same day — different environment (`dev-onprem-005`,
tenant `7e27253a-1ab8-4623-a789-56ae7e8939e9`) — cluster automation
(`automation/create_clusters.py` / `deploy_clusters.py`, see
`clusters/README.md`):**

- **Resolved the yeedu.yml token-injection ambiguity for good.** Auth
  against this new host kept failing with a misleading "session has
  expired" even with a genuinely fresh token. Root cause:
  `~/.yeedu/yeedu_cli.config` (a separate, single-token, host-unscoped
  cache written by any `yeedu configure` login) takes priority over
  `yeedu.yml` regardless of `YEEDU_RESTAPI_URL` — an earlier
  username/password login against `dev-onprem-008` in the same session
  had left its token cached there, and it was being silently sent to
  `dev-onprem-005` instead. Fixed: `configure_auth.inject_token()` now
  writes `yeedu_cli.config` directly (the file actually read), still
  mirrors into `yeedu.yml` for whatever secondary purpose it may serve.
  Added as skill gotcha #15.
- **`cluster create --cluster_type YEEDU` requires `--min_instances`/
  `--max_instances`** despite both being optional in `--help`:
  `{'error_code': 'RFA-000044', 'error_message': "The parameters
  'min_instances' and 'max_instances' are required for the 'YEEDU' and
  'STANDALONE' cluster types."}`. Added as skill gotcha #16.
- **`cluster create-conf` takes raw disk params** (`--disk_type_id --size
  --number_of_disks`), not a `volume_conf_id`, even though `cluster get`
  displays the result as a nested `machine_volume_conf` object — the
  platform creates/matches one internally.
- **Credential JSON shape for `Proxmox Basic Auth`** confirmed via
  round-trip: `base64(json.dumps({"USERNAME": ..., "PASSWORD": ...}))`.
  Password is write-only — `get-credential-conf` only ever echoes back
  `USERNAME`, confirming it can't be extracted from an existing
  credential to reuse; a caller-supplied dummy is used instead (see
  `clusters/README.md`).
- A freshly created, never-started cluster's `cluster_status` (and its
  node's `node_status`) reads `DESTROYED` — confirmed this is the normal
  "no compute provisioned" rest state, not a failure.
- Built and validated both create and idempotent-reuse paths live: created
  network-conf (id 69), boot-disk-image-conf (30), credential-conf (51),
  cloud-env (64), 4 cluster-confs (650–653), 4 clusters S/M/L/XL
  (359–362, all `DESTROYED`/not started as intended) — then re-ran the
  same script and confirmed every resource was found and reused, zero
  duplicates, zero errors.

## Known gaps — read before a real run

1. **Git-clone and tenant-associate REST shapes** are sourced only from the
   `yeedu-cli` skill's CLI-level docs (2.10.1-live-verified per its own
   header), not from an OpenAPI spec — consistent with everything else
   this script does, but worth knowing if something doesn't match.
2. **The "verify one notebook first" pause is now a soft check, not a hard
   guarantee** — content is force-pushed for every notebook on every run
   regardless, so even if the pause is skipped or answered wrong, content
   ends up correct on the *next* run. It's still worth checking the UI
   once per fresh Yeedu version/host in case the platform's behavior here
   changes again.
3. **No job has actually been run/executed** — every "confirmed" above is
   at the config-creation level (`job create` / `job get` accepted and
   stored what was expected, and now, additionally, the referenced files
   are confirmed to actually exist). Whether each job type *runs*
   correctly (JAR class resolves, Python script imports work, SQL
   executes) is still unverified — needs `--cluster-id` and `--start`, or
   manually starting from the Yeedu UI, against a live cluster.
4. **`git pull`'s completion is not verified the way `git clone`'s now
   is** — the idempotent "already cloned, pull latest" path calls `git
   pull` and trusts a non-error response, without polling any
   equivalent-to-clone-status check. Given how wrong that same assumption
   turned out to be for `git clone`, treat this as unverified until
   tested — if notebooks/jobs seem stale after a repo update, check this
   first.

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
| `provision.py` | Entrypoint — argparse, runs steps 1-6 in order, prints a summary. |
| `yeedu_client.py` | Subprocess wrapper: `YeeduClient.run(*args)` → parsed JSON, appends `--json-output default`, handles the exit-0-with-error-body and Python-repr quirks. `run_allow_not_found()` wraps idempotency-check calls, tolerating "not found" via a regex (`_NOT_FOUND_RE` — 4 distinct wordings seen live). `find_exact_match()` filters `search` results for an exact name match (search returns prefix matches too). |
| `configure_auth.py` | `inject_token()` (writes `~/.yeedu/yeedu_cli.config` — the file actually read, see "Confirmed live" gotcha #15 — and mirrors into `yeedu.yml`) or `login_with_credentials()` (`yeedu configure` with username/password) — either path, then an auth smoke test. |
| `deploy_clusters.py` / `create_clusters.py` | Separate from `provision.py` (different scope: cluster/cloud-env infra, no workspace or repo clone). Creates S/M/L/XL clusters on an OnPrem environment — see `clusters/README.md`. |
| `resolve_tenant_workspace.py` | `iam associate-tenant`, `workspace get`, and `create_workspace()` (auto-creates a new workspace per run when `--workspace-id` is omitted). |
| `clone_repo.py` | Async git clone with `clone-status` polling to actual completion (see "Confirmed live" — this used to silently no-op); idempotency requires `is_git: true` on the matched folder; also defines `REPO_WORKSPACE_PATH`, the in-workspace root (`/files/yeedu-demo-env-setup`) that `deploy_functions.py`/`create_notebooks.py` build paths from. |
| `deploy_functions.py` | Creates the 3 Functions jobs; strips version pins from `requirements.txt` per gotcha #4 (`--yeedu-functions-requirements` does a raw space-split, not JSON/comma parsing). |
| `deploy_other_jobs.py` | Creates the 3 remaining job types (`jobs/{jar,python,sql}`): JAR/Python use `job_command` (a path); SQL uses `--job-raw-scala-code <local file path>` instead — see "Confirmed live". |
| `create_notebooks.py` | Discovers `notebooks/**/*.ipynb`, creates (or reuses) each as a notebook, then force-pushes real content via `workspace create-workspace-file --overwrite true` every run (notebook create alone leaves it blank — see "Confirmed live"); pauses after the first for a manual UI check unless `--skip-notebook-confirm`. |

## Verify without a live instance

```bash
python3 -m py_compile automation/*.py
python3 automation/provision.py --dry-run --api-url https://x --token x --tenant-id x --workspace-id x
```
