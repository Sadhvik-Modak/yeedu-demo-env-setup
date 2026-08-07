"""Injects a pre-obtained Yeedu API token so `yeedu` CLI calls authenticate,
without going through `yeedu configure`'s username/password login flow.

UNVERIFIED for platform 2.10.1 — see automation/README.md "Known gaps" #1.
The `yeedu-cli` skill's auth-and-identity.md documents that
`~/.yeedu/yeedu.yml` holds a `yeedu_env` list of `{name, restapi_url,
token}` entries the CLI reads as pre-configured environments, but not the
exact selection mechanism (auto-match by restapi_url? most-recent-wins? an
explicit --env flag?). This module writes the entry and then smoke-tests it
with a real authenticated call (`iam get-user-info`) before the rest of the
pipeline proceeds.
"""
import os

import yaml

from yeedu_client import YeeduCommandError

YEEDU_DIR = os.path.expanduser("~/.yeedu")
YEEDU_YML_PATH = os.path.join(YEEDU_DIR, "yeedu.yml")
ENV_NAME = "yeedu-demo-env-setup-automation"


def inject_token(api_url, token):
    """Write api_url/token into ~/.yeedu/yeedu.yml and point YEEDU_RESTAPI_URL at it."""
    os.makedirs(YEEDU_DIR, exist_ok=True)

    config = {"yeedu_env": []}
    if os.path.exists(YEEDU_YML_PATH):
        with open(YEEDU_YML_PATH) as f:
            existing = yaml.safe_load(f) or {}
        config["yeedu_env"] = [
            e for e in existing.get("yeedu_env", []) if e.get("name") != ENV_NAME
        ]

    config["yeedu_env"].append({"name": ENV_NAME, "restapi_url": api_url, "token": token})

    with open(YEEDU_YML_PATH, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False)

    os.environ["YEEDU_RESTAPI_URL"] = api_url
    print(f"Wrote token to {YEEDU_YML_PATH} under env name '{ENV_NAME}'.")


def smoke_test(client):
    """Confirm the injected token actually authenticates. Raises on failure."""
    if client.dry_run:
        print("[dry-run] would smoke-test auth via `yeedu iam get-user-info`")
        return {"_dry_run": True}

    try:
        info = client.run("iam", "get-user-info")
    except YeeduCommandError as exc:
        raise RuntimeError(
            "Token injection via ~/.yeedu/yeedu.yml did not authenticate "
            "(see automation/README.md 'Known gaps' #1). Fall back to "
            "username/password: export YEEDU_USERNAME/YEEDU_PASSWORD and "
            "run `yeedu configure --no-browser=true` yourself, then re-run "
            "this script — it will reuse whatever session `yeedu configure` "
            "already established instead of re-injecting a token."
        ) from exc

    print(f"Auth OK — authenticated as: {info}")
    return info
