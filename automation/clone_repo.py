"""Idempotently clone this repo into the Yeedu workspace's file tree.

REPO_WORKSPACE_PATH is the assumed in-workspace path for everything cloned
here (`/files/<repo-folder-name>`, matching the convention shown in the
yeedu-cli skill's deploy-function.sh example). Both deploy_functions.py and
create_notebooks.py build their workspace paths off of it.
"""
from yeedu_client import get_field, run_allow_not_found

GIT_URL = "https://github.com/Sadhvik-Modak/yeedu-demo-env-setup.git"
GIT_PROVIDER = "GitHub"
REPO_FOLDER_NAME = "yeedu-demo-env-setup"
REPO_WORKSPACE_PATH = f"/files/{REPO_FOLDER_NAME}"


def clone_or_pull(client, workspace_id, git_branch="main"):
    print(f"Checking workspace {workspace_id} for an existing clone of {GIT_URL}...")
    existing = run_allow_not_found(
        client, "workspace", "list-workspace-files",
        "--workspace_id", str(workspace_id),
        "--is_dir", "true",
    )

    file_id = None
    if not client.dry_run:
        files = existing if isinstance(existing, list) else get_field(existing, "workspace_files", default=[])
        for f in (files or []):
            if isinstance(f, dict) and f.get("file_name") == REPO_FOLDER_NAME:
                file_id = f.get("file_id")
                break

    if file_id:
        print(f"Repo already cloned (file_id={file_id}) — pulling latest instead of re-cloning.")
        client.run("git", "pull", "--workspace_id", str(workspace_id), "--file_id", str(file_id))
        return file_id

    print(f"Cloning {GIT_URL} (branch {git_branch}) into workspace {workspace_id}...")
    result = client.run(
        "git", "clone",
        "--workspace_id", str(workspace_id),
        "--git_url", GIT_URL,
        "--git_provider", GIT_PROVIDER,
        "--git_branch", git_branch,
    )
    file_id = get_field(result, "file_id", "workspace_file_id", "id", default="<file_id unconfirmed, check yeedu workspace list-workspace-files>")
    print(f"Cloned — file_id={file_id}")
    return file_id
