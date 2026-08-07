"""Subprocess wrapper around the `yeedu` CLI, JSON in/out.

Targets Yeedu CLI/platform 2.10.1, driven via the `yeedu` console script
(package `yeedu-cli`) rather than raw HTTP — see automation/README.md for
why (the live authenticated OpenAPI spec for 2.10.1 wasn't reachable during
development; the CLI is the version-matched, quirk-documented interface).
"""
import ast
import json
import re
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
        # reliable success signal, the body must be inspected too. Also
        # confirmed: a validation failure (e.g. `job create --job-type
        # Spark SQL` with no --job-raw-scala-code) can come back as a bare
        # {"error": "..."} instead.
        #
        # NOT caught here, and NOT generically fixable: a THIRD shape,
        # {"message": "..."}, is used for both success ("Git clone repo
        # has been initiated.") and failure ("Please provide any one of
        # file_id or file_path...") with no other distinguishing field —
        # confirmed live via `git clone` silently no-op'ing without
        # --file_path and returning the latter with exit 0. There is no
        # reliable way to tell these apart from the response alone; any
        # caller relying on a bare "message" response for a
        # side-effecting call should independently verify the effect
        # actually happened (see clone_repo.py's clone-status polling for
        # a worked example) rather than trusting this wrapper.
        api_error = isinstance(parsed, dict) and (
            "error_code" in parsed or "error_message" in parsed or "error" in parsed
        )

        if result.returncode != 0 or api_error:
            message = None
            if isinstance(parsed, dict):
                message = parsed.get("error_message") or parsed.get("error")
            raise YeeduCommandError(
                full_args, result.returncode, result.stdout, result.stderr, message=message
            )

        return parsed


# Confirmed live (2026-08-07) against dev-onprem-008 — "not found" wording
# is NOT consistent across endpoints, all exit 0 with an error-shaped body:
# `job search` (nonexistent job): "...is not found within the Spark job
# for workspace id: 957"; `notebook search` (nonexistent notebook): "No
# notebook matches were found for the provided notebook name..."; `list-
# workspace-files` (empty/new workspace): "No workspace files found for
# the specified workspace ID: 959...". A plain substring list kept missing
# new phrasings each time, so this is a regex covering the shared
# skeleton: "not found" anywhere, or "no ... found" (any words between).
_NOT_FOUND_RE = re.compile(r"not\s+found|\bno\b.*\bfound\b", re.IGNORECASE)


def run_allow_not_found(client, *args):
    """Like `client.run(*args)`, but returns None instead of raising when
    the CLI's error body indicates a `search`/`list`/`get` miss (see
    _NOT_FOUND_RE). Idempotency checks need to treat that as "doesn't
    exist yet, go ahead and create it," not a fatal error."""
    try:
        return client.run(*args)
    except YeeduCommandError as exc:
        if _NOT_FOUND_RE.search(str(exc)):
            return None
        raise


def find_exact_match(result, name_field, name_value):
    """From a `search`-style response, return the entry whose `name_field`
    EXACTLY equals `name_value` — never just the first hit.

    Confirmed live (2026-08-07): `notebook search --notebook_name
    gold_citibike_station_activity_v1` returned BOTH that notebook and
    `gold_citibike_station_activity_v1_sql` (prefix match), in a `data`
    list with the *wrong* one first. Blindly taking result[0]
    (`get_field`'s list-handling) silently returns a different resource's
    id whenever one resource's name is a prefix of another's — exactly the
    `<name>` / `<name>_sql` pattern this repo's notebooks use throughout.
    """
    if result is None:
        return None
    entries = result.get("data") if isinstance(result, dict) and "data" in result else result
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if isinstance(entry, dict) and entry.get(name_field) == name_value:
            return entry
    return None


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
