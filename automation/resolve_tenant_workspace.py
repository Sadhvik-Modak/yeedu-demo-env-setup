"""Associate the session with a tenant and validate the target workspace."""
from yeedu_client import get_field


def associate_tenant(client, tenant_id):
    print(f"Associating tenant {tenant_id}...")
    client.run("iam", "associate-tenant", "--tenant_id", str(tenant_id))


def resolve_workspace(client, workspace_id):
    print(f"Resolving workspace {workspace_id}...")
    result = client.run("workspace", "get", "--workspace_id", str(workspace_id))
    name = get_field(result, "name", default="<unknown>")
    print(f"Workspace {workspace_id} resolved: {name}")
    return result
