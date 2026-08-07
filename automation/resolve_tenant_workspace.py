"""Associate the session with a tenant and resolve/create the target workspace."""
import datetime

from yeedu_client import get_field


def associate_tenant(client, tenant_id):
    print(f"Associating tenant {tenant_id}...")
    client.run("iam", "associate-tenant", "--tenant_id", str(tenant_id))


def resolve_workspace(client, workspace_id):
    print(f"Resolving workspace {workspace_id}...")
    result = client.run("workspace", "get", "--workspace_id", str(workspace_id))
    name = get_field(result, "name", default="<unknown>")
    print(f"Workspace {workspace_id} resolved: {name}")
    return workspace_id


def create_workspace(client, name=None):
    """No --workspace-id given: create a fresh workspace for this run
    (not idempotent by design — user asked for a new workspace every run
    when one isn't explicitly provided)."""
    if name is None:
        name = "yeedu-demo-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    print(f"No --workspace-id given — creating a new workspace '{name}'...")
    result = client.run("workspace", "create", "--name", name)
    workspace_id = get_field(result, "workspace_id", default="<dry-run-workspace-id>")
    print(f"Created workspace_id={workspace_id} ('{name}')")
    return workspace_id
