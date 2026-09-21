"""Guarded, staging-only chart config editor for the edit-faust-metadata skill.

Edits a chart's config through the admin API of the branch's STAGING server. The guard is
hard-coded: it refuses master/main and asserts the resolved environment is staging — there is
no production escape hatch, by design. Staging chart edits reach production only via
chart-diff approval + chart-sync after the PR merges.

It GETs the chart's PATCH config (the set of fields explicitly set at chart level), applies
the requested changes, prints the diff, and PUTs the config back. The grapher server re-derives
patch and full on PUT, so:

- `--set key=value` overrides a field at chart level (shadowing any inherited value);
- `--unset key` deletes the key — the server re-inherits the indicator's value if there is one.

Usage:
    .venv/bin/python .claude/skills/edit-faust-metadata/scripts/update_chart_config.py \
        --branch <b> --chart-id <id> \
        [--set subtitle='New subtitle' ...] [--unset note ...] \
        [--set-json selectedEntityNames='["France","Japan"]' ...] \
        [--dry-run]

Dot-paths are supported for nested keys (`map.time=latest`, `dimensions.0.display.name=Label`).
Run with --dry-run first, show the user the diff, then apply.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from apps.chart_sync.admin_api import AdminAPI  # noqa: E402
from etl.config import OWIDEnv, get_container_name  # noqa: E402


def current_branch() -> str:
    return subprocess.check_output(["git", "branch", "--show-current"], cwd=REPO_ROOT, text=True).strip()


def guarded_staging_env(branch: str) -> OWIDEnv:
    """The staging-only guard. No flag disables it — production is never a valid target here."""
    if branch in ("master", "main", ""):
        raise SystemExit(
            "Refusing to edit charts on master/main — chart edits go to a work branch's staging "
            "server only. Create the branch first (`etl pr ...`)."
        )
    env = OWIDEnv.from_staging(branch)
    assert env.env_remote == "staging", (
        f"Resolved environment is '{env.env_remote}', not staging — aborting before any write."
    )
    return env


def _navigate(config: dict, path: str) -> tuple[Any, str | int]:
    """Walk a dot-path, creating intermediate dicts; return (container, last_key)."""
    parts = path.split(".")
    node: Any = config
    for i, part in enumerate(parts[:-1]):
        key: str | int = int(part) if part.isdigit() and isinstance(node, list) else part
        if isinstance(node, list):
            node = node[key]  # type: ignore[index]
        else:
            if part not in node:
                node[part] = {}
            node = node[part]
        if not isinstance(node, (dict, list)):
            raise SystemExit(f"Cannot descend into '{'.'.join(parts[: i + 1])}' — it holds a scalar.")
    last = parts[-1]
    return node, (int(last) if last.isdigit() and isinstance(node, list) else last)


def get_value(config: dict, path: str) -> Any:
    node: Any = config
    for part in path.split("."):
        key: str | int = int(part) if part.isdigit() and isinstance(node, list) else part
        try:
            node = node[key]  # type: ignore[index]
        except (KeyError, IndexError, TypeError):
            return None
    return node


def parse_assignment(arg: str) -> tuple[str, str]:
    if "=" not in arg:
        raise SystemExit(f"--set/--set-json expects key=value, got '{arg}'")
    key, value = arg.split("=", 1)
    return key.strip(), value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--branch", default=None, help="Work branch (defaults to current git branch)")
    parser.add_argument("--chart-id", type=int, required=True)
    parser.add_argument("--set", dest="sets", action="append", default=[], help="key=string value (repeatable)")
    parser.add_argument("--set-json", dest="set_jsons", action="append", default=[], help="key=JSON value (repeatable)")
    parser.add_argument("--unset", dest="unsets", action="append", default=[], help="key to delete (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="Print the diff and stop before writing")
    args = parser.parse_args()

    if not (args.sets or args.set_jsons or args.unsets):
        raise SystemExit("Nothing to do — pass --set / --set-json / --unset.")

    branch = args.branch or current_branch()
    env = guarded_staging_env(branch)
    print(f"Target: {env.admin_api} (staging-site container: {get_container_name(branch)})")

    api = AdminAPI(env)
    config = api.get_chart_config(args.chart_id)

    changes: list[tuple[str, Any, Any]] = []  # (path, before, after)
    for arg in args.sets:
        path, raw = parse_assignment(arg)
        before = get_value(config, path)
        node, key = _navigate(config, path)
        node[key] = raw
        changes.append((path, before, raw))
    for arg in args.set_jsons:
        path, raw = parse_assignment(arg)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as e:
            raise SystemExit(f"--set-json value for '{path}' is not valid JSON: {e}") from e
        before = get_value(config, path)
        node, key = _navigate(config, path)
        node[key] = value
        changes.append((path, before, value))
    for path in args.unsets:
        before = get_value(config, path)
        if before is None:
            print(f"! '{path}' is not set at chart level — nothing to unset (already inherited or absent).")
            continue
        node, key = _navigate(config, path)
        del node[key]
        changes.append((path, before, "<unset — re-inherits>"))

    print("\nChanges:")
    for path, before, after in changes:
        print(f"  {path}:")
        print(f"    before: {json.dumps(before, ensure_ascii=False)}")
        print(f"    after:  {json.dumps(after, ensure_ascii=False, default=str)}")

    if args.dry_run:
        print("\n--dry-run: no write performed.")
        return

    resp = api.update_chart(args.chart_id, config)
    if not resp.get("success"):
        raise SystemExit(f"Admin API reported failure: {resp}")
    slug = config.get("slug")
    site = f"http://{get_container_name(branch)}"
    print("\nOK — chart updated on staging.")
    if slug:
        print(f"Verify: {site}/grapher/{slug} (SVG: {site}/grapher/{slug}.svg)")
    print(f"Edit page: {site}/admin/charts/{args.chart_id}/edit")
    print("This change rides to production via chart-diff approval + merge.")


if __name__ == "__main__":
    main()
