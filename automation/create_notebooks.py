"""Register each notebooks/**/*.ipynb as a Yeedu notebook.

UNVERIFIED — see automation/README.md "Known gaps" #2. Assumes
`notebook create --notebook_path <path>` adopts an existing .ipynb file
already present at that workspace path (from the git clone step) rather
than creating a blank notebook — no confirmed schema/example says this
either way. To de-risk, this creates and pauses on ONE notebook first so
the caller can check the Yeedu UI before batch-creating the rest.
"""
import os

from clone_repo import REPO_WORKSPACE_PATH
from yeedu_client import get_field, run_allow_not_found


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
    return get_field(result, "notebook_id")


def _create_one(client, workspace_id, nb, cluster_id):
    workspace_path = f"{REPO_WORKSPACE_PATH}/{nb['rel_path']}"
    existing_id = _find_notebook_id(client, workspace_id, nb["notebook_name"])
    if existing_id:
        print(f"  already exists (notebook_id={existing_id}) — reusing.")
        return existing_id

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
    return notebook_id


def create_all(client, repo_root, workspace_id, cluster_id=None, skip_confirm=False):
    notebooks = _discover_notebooks(repo_root)
    if not notebooks:
        print("No .ipynb files found under notebooks/ — nothing to do.")
        return []

    print(f"Found {len(notebooks)} notebooks.")

    first, rest = notebooks[0], notebooks[1:]
    print(f"\nVerify-first: creating '{first['notebook_name']}' ({first['notebook_type']}) alone first.")
    first_id = _create_one(client, workspace_id, first, cluster_id)
    created = [{"notebook_name": first["notebook_name"], "notebook_id": first_id}]

    if not client.dry_run and not skip_confirm:
        print(
            "\nSTOP AND CHECK in the Yeedu UI that this notebook shows the "
            "real cloned cell content, not an empty notebook (see "
            "automation/README.md 'Known gaps' #2)."
        )
        answer = input(
            f"Confirmed OK — create the remaining {len(rest)} notebooks now? [y/N] "
        ).strip().lower()
        if answer != "y":
            print(
                "Stopping here. Re-run with --skip-notebook-confirm once "
                "you've verified once, or re-run as-is to check again — "
                "the first notebook will be reused, not duplicated."
            )
            return created

    for nb in rest:
        print(f"\n{nb['notebook_name']} ({nb['notebook_type']})")
        nb_id = _create_one(client, workspace_id, nb, cluster_id)
        created.append({"notebook_name": nb["notebook_name"], "notebook_id": nb_id})
    return created
