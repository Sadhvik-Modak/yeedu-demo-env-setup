"""Register each notebooks/**/*.ipynb as a Yeedu notebook.

CONFIRMED LIVE (2026-08-07): `notebook create --notebook_path <path>` does
NOT adopt an existing file's content — it overwrites whatever is at that
path with a blank one-cell skeleton, even when a real git-cloned .ipynb was
already there (confirmed by downloading and diffing: a 4054-byte real
notebook became a 742-byte blank one). Fix: after create (or reuse), always
push the real local content via `workspace create-workspace-file
--overwrite true`, which preserves the notebook<->file_id binding while
replacing the content.
"""
import os

from clone_repo import REPO_WORKSPACE_PATH
from yeedu_client import find_exact_match, get_field, run_allow_not_found


def _discover_notebooks(repo_root):
    notebooks_dir = os.path.join(repo_root, "notebooks")
    found = []
    for root, _dirs, files in sorted(os.walk(notebooks_dir)):
        for fname in sorted(files):
            if not fname.endswith(".ipynb"):
                continue
            rel_path = os.path.relpath(os.path.join(root, fname), repo_root).replace(os.sep, "/")
            notebook_type = "sql" if fname.endswith("_sql.ipynb") else "python3"
            notebook_name = fname[: -len(".ipynb")]
            found.append({"rel_path": rel_path, "notebook_name": notebook_name, "notebook_type": notebook_type})
    return found


def _find_notebook_id(client, workspace_id, notebook_name):
    result = run_allow_not_found(
        client, "notebook", "search", "--workspace_id", str(workspace_id), "--notebook_name", notebook_name
    )
    match = find_exact_match(result, "notebook_name", notebook_name)
    return match.get("notebook_id") if match else None


def _push_content(client, workspace_id, local_path, workspace_path):
    """Overwrite the notebook's backing file with real local content.
    Preserves the notebook<->file_id binding (confirmed live) — only the
    file's bytes change."""
    root_output_dir = os.path.dirname(workspace_path)
    client.run(
        "workspace", "create-workspace-file",
        "--workspace_id", str(workspace_id),
        "--local_file_path", local_path,
        "--root_output_dir", root_output_dir,
        "--overwrite", "true",
    )


def _create_one(client, repo_root, workspace_id, nb, cluster_id):
    workspace_path = f"{REPO_WORKSPACE_PATH}/{nb['rel_path']}"
    local_path = os.path.join(repo_root, nb["rel_path"])

    existing_id = _find_notebook_id(client, workspace_id, nb["notebook_name"])
    if existing_id:
        print(f"  already exists (notebook_id={existing_id}) — reusing, re-pushing content.")
        notebook_id = existing_id
    else:
        args = [
            "notebook", "create",
            "--workspace_id", str(workspace_id),
            "--notebook_name", nb["notebook_name"],
            "--notebook_type", nb["notebook_type"],
            "--notebook_path", workspace_path,
        ]
        if cluster_id:
            args += ["--cluster_id", str(cluster_id)]
        result = client.run(*args)
        notebook_id = get_field(result, "notebook_id", default="<dry-run-notebook-id>")
        print(f"  created notebook_id={notebook_id} at {workspace_path}")

    if not client.dry_run:
        _push_content(client, workspace_id, local_path, workspace_path)
        print(f"  pushed real content ({os.path.getsize(local_path)} bytes) to {workspace_path}")
    else:
        print(f"[dry-run] would push real content from {local_path} to {workspace_path}")

    return notebook_id


def create_all(client, repo_root, workspace_id, cluster_id=None, skip_confirm=False):
    notebooks = _discover_notebooks(repo_root)
    if not notebooks:
        print("No .ipynb files found under notebooks/ — nothing to do.")
        return []

    print(f"Found {len(notebooks)} notebooks.")

    first, rest = notebooks[0], notebooks[1:]
    print(f"\nVerify-first: creating '{first['notebook_name']}' ({first['notebook_type']}) alone first.")
    first_id = _create_one(client, repo_root, workspace_id, first, cluster_id)
    created = [{"notebook_name": first["notebook_name"], "notebook_id": first_id}]

    if not client.dry_run and not skip_confirm:
        print(
            "\nSTOP AND CHECK in the Yeedu UI that this notebook shows the "
            "real cloned cell content, not an empty notebook."
        )
        answer = input(
            f"Confirmed OK — create the remaining {len(rest)} notebooks now? [y/N] "
        ).strip().lower()
        if answer != "y":
            print(
                "Stopping here. Re-run with --skip-notebook-confirm once "
                "you've verified once, or re-run as-is to check again — "
                "existing notebooks are reused, not duplicated (content is "
                "re-pushed every run either way, so it self-heals)."
            )
            return created

    for nb in rest:
        print(f"\n{nb['notebook_name']} ({nb['notebook_type']})")
        nb_id = _create_one(client, repo_root, workspace_id, nb, cluster_id)
        created.append({"notebook_name": nb["notebook_name"], "notebook_id": nb_id})
    return created
