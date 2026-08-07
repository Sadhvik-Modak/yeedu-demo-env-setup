"""Injects a pre-obtained Yeedu API token so `yeedu` CLI calls authenticate,
without going through `yeedu configure`'s username/password login flow.

CONFIRMED LIVE (2026-08-07) — resolves what was previously Known Gap #1:
the file that actually matters is `~/.yeedu/yeedu_cli.config`
(`{"token": "<jwt>"}`, no host scoping at all), NOT `~/.yeedu/yeedu.yml`.
A prior `yeedu configure` login (even hours earlier, against a completely
different host) leaves a cached session in `yeedu_cli.config` that
silently overrides anything written to `yeedu.yml` and gets sent
regardless of `YEEDU_RESTAPI_URL` — surfacing as a misleading "session has
expired" error even for a genuinely fresh token for the *intended* host.
See `~/.claude/skills/yeedu-cli/reference/known-issues-and-gotchas.md`
gotcha #15 for the full incident writeup. This module now writes the
token to `yeedu_cli.config` (the file actually read) and also mirrors it
into `yeedu.yml` as a `yeedu_env` entry (harmless, kept for whatever
multi-environment selection mechanism that file may still serve — still
otherwise unconfirmed).
"""
import json
import os
import subprocess

import yaml

from yeedu_client import YeeduCommandError

YEEDU_DIR = os.path.expanduser("~/.yeedu")
YEEDU_YML_PATH = os.path.join(YEEDU_DIR, "yeedu.yml")
YEEDU_CLI_CONFIG_PATH = os.path.join(YEEDU_DIR, "yeedu_cli.config")
ENV_NAME = "yeedu-onprem"


def inject_token(api_url, token):
    """Write api_url/token to ~/.yeedu/yeedu_cli.config (the file the CLI
    actually reads — see module docstring), mirror into yeedu.yml, and
    point YEEDU_RESTAPI_URL at it."""
    os.makedirs(YEEDU_DIR, exist_ok=True)

    with open(YEEDU_CLI_CONFIG_PATH, "w") as f:
        json.dump({"token": token}, f)
    print(f"Wrote token to {YEEDU_CLI_CONFIG_PATH} (overwrites any previously cached session for a different host).")

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


def login_with_credentials(api_url, username, password, dry_run=False):
    """Log in via `yeedu configure` (username/password), instead of token
    injection. Confirmed live (2026-08-07) as the reliable path when a
    fresh token isn't in hand — see README "Confirmed live". Note this
    also writes ~/.yeedu/yeedu_cli.config, so a subsequent inject_token()
    call for a *different* host in the same session correctly overwrites
    it (see module docstring)."""
    if dry_run:
        print(f"[dry-run] would `yeedu configure --no-browser=true` as {username} against {api_url}")
        os.environ["YEEDU_RESTAPI_URL"] = api_url
        return

    env = os.environ.copy()
    env["YEEDU_RESTAPI_URL"] = api_url
    env["YEEDU_USERNAME"] = username
    env["YEEDU_PASSWORD"] = password

    print(f"Logging in to {api_url} as {username}...")
    result = subprocess.run(
        ["yeedu", "configure", "--no-browser=true"],
        env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"`yeedu configure` failed (exit {result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    print("Login OK.")
    os.environ["YEEDU_RESTAPI_URL"] = api_url


def smoke_test(client):
    """Confirm the injected token actually authenticates. Raises on failure."""
    if client.dry_run:
        print("[dry-run] would smoke-test auth via `yeedu iam get-user-info`")
        return {"_dry_run": True}

    try:
        info = client.run("iam", "get-user-info")
    except YeeduCommandError as exc:
        raise RuntimeError(
            "Auth did not succeed (see automation/README.md 'Confirmed "
            "live' and the yeedu-cli skill's gotcha #15 — check whether "
            "~/.yeedu/yeedu_cli.config holds a stale session for a "
            "*different* host from an earlier login in this session). "
            "Fall back to username/password: export "
            "YEEDU_USERNAME/YEEDU_PASSWORD and run `yeedu configure "
            "--no-browser=true` yourself, then re-run this script."
        ) from exc

    print(f"Auth OK — authenticated as: {info}")
    return info
