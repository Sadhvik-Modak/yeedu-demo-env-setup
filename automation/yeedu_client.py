"""Subprocess wrapper around the `yeedu` CLI, JSON in/out.

Targets Yeedu CLI/platform 2.10.1, driven via the `yeedu` console script
(package `yeedu-cli`) rather than raw HTTP — see automation/README.md for
why (the live authenticated OpenAPI spec for 2.10.1 wasn't reachable during
development; the CLI is the version-matched, quirk-documented interface).
"""
import ast
import json
import subprocess


class YeeduCommandError(RuntimeError):
    def __init__(self, args, returncode, stdout, stderr, message=None):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        detail = message or stderr.strip() or stdout.strip()
        super().__init__(f"yeedu {' '.join(args)} failed (exit {returncode}): {detail}")


class YeeduClient:
    """Runs `yeedu <args...>` and parses JSON output.

    Every call appends `--json-output default` (compact JSON, not the
    pretty-printed default) so output is machine-parseable.

    In dry-run mode, no subprocess is spawned — the command is printed and
    a stub `{"_dry_run": True}` is returned so calling code can flow
    through without special-casing every step.
    """

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.log = []

    def run(self, *args):
        full_args = ["yeedu", *args, "--json-output", "default"]
        self.log.append(full_args)

        if self.dry_run:
            print("[dry-run] " + " ".join(full_args))
            return {"_dry_run": True}

        result = subprocess.run(full_args, capture_output=True, text=True, check=False)
        stdout = result.stdout.strip()

        parsed = None
        if stdout:
            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError:
                # Confirmed live against dev-onprem-008: despite the flag
                # name, `--json-output default` can emit Python dict/list
                # repr (single-quoted) rather than real JSON — e.g.
                # {'error_code': 'RFA-000001', 'error_message': '...'}.
                # ast.literal_eval safely parses that; only bare text (e.g.
                # `logs` output) falls through to the raw-string case.
                try:
                    parsed = ast.literal_eval(stdout)
                except (ValueError, SyntaxError):
                    parsed = stdout

        # Confirmed live against dev-onprem-008: an expired/invalid token
        # still exits 0 with a JSON *body* like
        # {"error_code": "RFA-000001", "error_message": "User session has
        # expired. Please login again."} — exit code alone is not a
        # reliable success signal, the body must be inspected too.
        api_error = isinstance(parsed, dict) and ("error_code" in parsed or "error_message" in parsed)

        if result.returncode != 0 or api_error:
            message = parsed.get("error_message") if isinstance(parsed, dict) else None
            raise YeeduCommandError(
                full_args, result.returncode, result.stdout, result.stderr, message=message
            )

        return parsed


def get_field(obj, *keys, default=None):
    """Best-effort field extraction from a `yeedu` CLI JSON response.

    Response shapes aren't confirmed for 2.10.1 (no live instance available
    during development) — some commands may return a bare object, others a
    list, others a wrapper like {"data": {...}}. Try common shapes
    defensively instead of assuming one; re-check against real output the
    first time this runs live.
    """
    if obj is None or (isinstance(obj, dict) and obj.get("_dry_run")):
        return default
    if isinstance(obj, list) and obj:
        obj = obj[0]
    if isinstance(obj, dict):
        if "data" in obj and isinstance(obj["data"], (dict, list)):
            return get_field(obj["data"], *keys, default=default)
        for key in keys:
            if key in obj:
                return obj[key]
    return default
