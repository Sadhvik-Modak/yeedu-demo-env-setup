"""Idempotently clone this repo into the Yeedu workspace's file tree.

CONFIRMED LIVE (2026-08-07) — this was a serious, previously-undetected
bug: `git clone` silently no-ops without `--file_id` or `--file_path` (a
REQUIRED destination parent — the yeedu-cli skill's docs listed both as
optional, which is wrong). Without it the API returns exit 0 with
`{'message': 'Please provide any one of file_id or file_path to clone a
repo into a workspace.'}` — no `error_code`/`error_message` fields, so it
slipped straight past `YeeduClient`'s error detection and was silently
treated as success. Every job this script created before this fix pointed
at files that were never actually cloned in; only `notebooks/` existed in
past runs, created as a side effect of `create_notebooks.py`'s own
content-push calls, not by `git clone` at all — see
`automation/README.md` "Confirmed live" for the full incident writeup.

Also confirmed: `git clone` is ASYNC. The initial response is just
`{'message': 'Git clone repo has been initiated.', 'workspace_file_id':
...}` (note: `workspace_file_id`, not `file_id` — the old idempotency
check here used the wrong key and silently got `None`, a second bug).
This module now passes `--file_path /` and polls `git clone-status` until
`COMPLETED` (or raises on failure/timeout) before returning, so callers
can actually trust the repo is present.

REPO_WORKSPACE_PATH is the assumed in-workspace path for everything cloned
here (`/files/<repo-folder-name>`, matching the convention shown in the
yeedu-cli skill's deploy-function.sh example). Both deploy_functions.py and
create_notebooks.py build their workspace paths off of it.
"""
import time

from yeedu_client import get_field, run_allow_not_found

GIT_URL = "https://github.com/Sadhvik-Modak/yeedu-demo-env-setup.git"
GIT_PROVIDER = "GitHub"
REPO_FOLDER_NAME = "yeedu-demo-env-setup"
REPO_WORKSPACE_PATH = f"/files/{REPO_FOLDER_NAME}"

CLONE_POLL_INTERVAL_SEC = 5
CLONE_POLL_TIMEOUT_SEC = 300
TERMINAL_STATUSES = {"COMPLETED", "ERROR", "FAILED"}


def _find_existing_git_folder(client, workspace_id):
    """Only a genuinely git-tracked folder counts as "already cloned" —
    a same-named plain folder (e.g. left over from the bug described in
    the module docstring) must not be mistaken for one, or `git pull`
    would be run against a folder git never touched."""
    existing = run_allow_not_found(
        client, "workspace", "list-workspace-files",
        "--workspace_id", str(workspace_id),
        "--is_dir", "true",
    )
    if client.dry_run:
        return None
    files = existing if isinstance(existing, list) else get_field(existing, "workspace_files", default=[])
    for f in (files or []):
        if isinstance(f, dict) and f.get("file_name") == REPO_FOLDER_NAME and f.get("is_git"):
            return f.get("workspace_file_id") or f.get("file_id")
    return None


def _wait_for_clone(client, workspace_id, file_id):
    if client.dry_run:
        print("[dry-run] would poll `yeedu git clone-status` until COMPLETED")
        return
    print(f"Waiting for clone (file_id={file_id}) to complete...")
    deadline = time.monotonic() + CLONE_POLL_TIMEOUT_SEC
    status = None
    while time.monotonic() < deadline:
        result = client.run(
            "git", "clone-status", "--workspace_id", str(workspace_id), "--file_id", str(file_id)
        )
        status = get_field(result, "job_status", default=None)
        print(f"  clone status: {status}")
        if status in TERMINAL_STATUSES:
            break
        time.sleep(CLONE_POLL_INTERVAL_SEC)
    if status != "COMPLETED":
        raise RuntimeError(
            f"git clone did not complete successfully (final status: {status}). Check: "
            f"yeedu git clone-log --workspace_id {workspace_id} --file_id {file_id} --log_type stderr"
        )
    print("Clone completed.")


def clone_or_pull(client, workspace_id, git_branch="main"):
    print(f"Checking workspace {workspace_id} for an existing git clone of {GIT_URL}...")
    existing_file_id = _find_existing_git_folder(client, workspace_id)

    if existing_file_id:
        print(f"Repo already cloned (file_id={existing_file_id}) — pulling latest instead of re-cloning.")
        client.run("git", "pull", "--workspace_id", str(workspace_id), "--file_id", str(existing_file_id))
        return existing_file_id

    print(f"Cloning {GIT_URL} (branch {git_branch}) into workspace {workspace_id}...")
    result = client.run(
        "git", "clone",
        "--workspace_id", str(workspace_id),
        "--file_path", "/",
        "--git_url", GIT_URL,
        "--git_provider", GIT_PROVIDER,
        "--git_branch", git_branch,
        "--git_folder_name", REPO_FOLDER_NAME,
    )
    file_id = get_field(result, "workspace_file_id", "file_id", default=None)
    if not client.dry_run and not file_id:
        raise RuntimeError(f"git clone did not return a workspace_file_id — response: {result!r}")

    _wait_for_clone(client, workspace_id, file_id)
    return file_id
