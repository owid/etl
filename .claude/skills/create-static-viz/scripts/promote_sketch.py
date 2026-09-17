"""Promote a static-viz sketch into a real `viz://static` step.

Copies `<sketch-dir>/sketch.py` to `etl/steps/viz/static/<ns>/<version>/<short>.py`, swaps the
`SketchPaths(__file__)` line for `PathFinder(__file__)`, fixes the imports, appends the DAG entry to
`dag/static_viz.yml`, and prints the edits that still need a human: the loader and the source
citation. Nothing else in the sketch is rewritten — its docstring, and the Figma handoff recorded in
it, travel with the file.

Usage:
    promote_sketch.py <sketch-dir> --step viz://static/<ns>/<version>/<short> --dep data://garden/... [--dep ...]
                      [--comment "<dag comment>"] [--dry-run] [--force] [--repo-root <dir>]

Run it INSIDE the `etl pr "<title>" data --worktree` checkout: it refuses to write on master, main or
a detached HEAD. The sketch usually lives in the main checkout's gitignored `ai/`, so pass an absolute
path — or create the worktree with `--share-data`, which symlinks `ai/` in.
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[4]
DAG_FILE = Path("dag/static_viz.yml")
STEPS_DIR = Path("etl/steps/viz/static")
MAX_LINE = 120

STEP_URI = re.compile(r"^viz://static/([a-z0-9_]+)/(\d{4}-\d{2}-\d{2}|latest)/([a-z0-9_]+)$")
# The scaffold's line, with or without its trailing `# PROMOTE` comment.
PATHS_LINE = re.compile(r"^paths = SketchPaths\(__file__\)(?:\s*#.*)?$", re.MULTILINE)
# Single-line or parenthesised (`[^)]` spans newlines), as ruff may have wrapped it.
STATIC_IMPORT = re.compile(r"^from etl\.viz\.static import (\([^)]*\)|[^\n]+)$", re.MULTILINE)
HELPERS_IMPORT = re.compile(r"^from etl\.helpers import ([^\n(]+)$", re.MULTILINE)
# The scaffold's `TITLE = <string literal>` line, in either quote style (`ruff format` picks one).
TITLE_LINE = re.compile(r"""^TITLE = ("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')$""", re.MULTILINE)
# The first key of the scaffold's LAYOUTS registry: the slug, which names the unsuffixed frame.
LAYOUTS_FIRST_KEY = re.compile(r'^LAYOUTS = \{\n\s+"([a-z0-9_]+)": \{', re.MULTILINE)


class PromoteError(Exception):
    """A refusal: nothing has been written when this is raised."""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sketch_dir", type=Path, help="the sketch directory holding sketch.py")
    parser.add_argument("--step", required=True, help="viz://static/<namespace>/<version>/<short_name>")
    parser.add_argument("--dep", action="append", required=True, dest="deps", help="a data:// dependency; repeatable")
    parser.add_argument("--comment", help="the one-line DAG comment; defaults to the sketch's TITLE")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and write nothing")
    parser.add_argument("--force", action="store_true", help="overwrite an existing step file")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT, help="checkout to write into (default: this one)")
    args = parser.parse_args()

    try:
        return promote(args)
    except PromoteError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


def promote(args: argparse.Namespace) -> int:
    repo_root = args.repo_root.resolve()
    sketch_dir = resolve_sketch_dir(args.sketch_dir, repo_root)
    sketch_path = sketch_dir / "sketch.py"
    if not sketch_path.is_file():
        raise PromoteError(f"{sketch_dir} holds no sketch.py")

    m = STEP_URI.match(args.step)
    if not m:
        raise PromoteError(f"--step {args.step!r} must match {STEP_URI.pattern}")
    namespace, version, short_name = m.groups()
    target = repo_root / STEPS_DIR / namespace / version / f"{short_name}.py"
    dag_path = repo_root / DAG_FILE
    if not dag_path.is_file():
        raise PromoteError(f"{dag_path} not found; is --repo-root an etl checkout?")

    if not args.dry_run:
        branch = current_branch(repo_root)
        if branch in ("master", "main", ""):
            raise PromoteError(
                f"on {branch or 'a detached HEAD'!r}: promotion happens inside a PR branch. Run "
                '`.venv/bin/etl pr "<title>" data --worktree --share-data` first (WRITING-THE-STEP.md), '
                "then promote from that checkout."
            )
    if target.exists() and not args.force:
        raise PromoteError(f"{target} exists; pass --force to overwrite")

    dag_text = dag_path.read_text()
    steps = (yaml.safe_load(dag_text) or {}).get("steps") or {}
    if args.step in steps:
        raise PromoteError(f"{args.step} is already in {DAG_FILE}")

    source = sketch_path.read_text()
    check_slug(source, short_name)
    promoted, manual = rewrite_source(source)
    comment = args.comment or sketch_title(source) or short_name
    new_dag_text = dag_text.rstrip("\n") + "\n\n" + dag_block(args.step, args.deps, comment)
    new_steps = yaml.safe_load(new_dag_text)["steps"]
    assert set(steps) <= set(new_steps) and new_steps[args.step] == args.deps, (
        "the DAG append changed more than it added"
    )

    rel_target = target.relative_to(repo_root)
    if args.dry_run:
        print(f"DRY RUN — would write {rel_target} and append to {DAG_FILE}:\n")
        print(dag_block(args.step, args.deps, comment))
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(promoted)
        dag_path.write_text(new_dag_text)
        print(f"Wrote {rel_target} and appended {args.step} to {DAG_FILE}.")

    dep_short = args.deps[0].rsplit("/", 1)[-1]
    print("\nLeft for you, in this order:")
    for i, item in enumerate(
        manual
        + [
            f"run `.venv/bin/etlr {args.step}`, then `verify_static_viz.py {rel_target.parent} --template <name> --expect-gid <data layer>`"
            " (the commit hook runs `make check`; do not run it separately)",
            "now run the normal flow: Steps 1-2 (resolve the dependency, check for newer data) and 5-9, plus "
            "/owid-staff:create-figma-chart's finalize mode on the sketch frame",
            "do not commit the sketch's own PNG/SVG: the step re-renders its pair next to the .py",
        ],
        start=1,
    ):
        print(f"  {i}. {item.replace('<short_name>', dep_short)}")
    return 0


def resolve_sketch_dir(given: Path, repo_root: Path) -> Path:
    if given.is_absolute():
        return given
    for base in (Path.cwd(), repo_root, REPO_ROOT):
        if (base / given / "sketch.py").is_file():
            return (base / given).resolve()
    return given.resolve()


def current_branch(repo_root: Path) -> str:
    return subprocess.check_output(["git", "branch", "--show-current"], cwd=repo_root, text=True).strip()


def check_slug(source: str, short_name: str) -> None:
    """The slug becomes the step's short_name (new_sketch.py's own rule): the frames and the LAYOUTS
    keys carry it, and only the file is renamed here, so a different `--step` short name would leave
    the step and its Figma handoff under two identities."""
    m = LAYOUTS_FIRST_KEY.search(source)
    if not m:
        raise PromoteError("cannot read the slug from the LAYOUTS registry — is this a new_sketch.py scaffold?")
    if m.group(1) != short_name:
        raise PromoteError(
            f"the sketch's slug is {m.group(1)!r} but --step names {short_name!r}: the slug becomes the step's "
            "short_name (its frames and LAYOUTS keys carry it). Use --step .../"
            f"{m.group(1)}, or re-scaffold under the new slug."
        )


def sketch_title(source: str) -> str | None:
    """The sketch's TITLE, unescaped — it may hold quotes, which the scaffold writes as a literal."""
    m = TITLE_LINE.search(source)
    return ast.literal_eval(m.group(1)) if m else None


def rewrite_source(source: str) -> tuple[str, list[str]]:
    """Apply the two mechanical edits; return the new source and the manual edits left.

    Every replacement is guarded (`GOTCHAS.md` -> scripted edits): a pattern that matches nothing or
    twice raises instead of silently returning the source unchanged.
    """
    paths_lines = PATHS_LINE.findall(source)
    if len(paths_lines) != 1:
        raise PromoteError(
            f"expected exactly one `paths = SketchPaths(__file__)` line, found {len(paths_lines)} — "
            "is this a new_sketch.py scaffold?"
        )
    source = PATHS_LINE.sub("paths = PathFinder(__file__)", source, count=1)

    imports = list(STATIC_IMPORT.finditer(source))
    if len(imports) != 1:
        raise PromoteError(f"expected exactly one `from etl.viz.static import ...`, found {len(imports)}")
    names = [n.strip() for n in imports[0].group(1).strip("()").replace("\n", ",").split(",") if n.strip()]
    if "SketchPaths" not in names:
        raise PromoteError("the etl.viz.static import does not name SketchPaths — not a scaffold, or already promoted")
    names.remove("SketchPaths")
    static_import = format_import("etl.viz.static", names)

    helpers = HELPERS_IMPORT.search(source)
    if helpers:
        helper_names = sorted({*[n.strip() for n in helpers.group(1).split(",")], "PathFinder"})
        source = source[: helpers.start()] + format_import("etl.helpers", helper_names) + source[helpers.end() :]
        imports = list(STATIC_IMPORT.finditer(source))
        replacement = static_import
    else:
        replacement = format_import("etl.helpers", ["PathFinder"]) + "\n" + static_import
    source = source[: imports[0].start()] + replacement + source[imports[0].end() :]

    if "SketchPaths" in source:
        raise PromoteError(
            "SketchPaths is still referenced after the swap — the sketch uses it somewhere the scaffold does not"
        )

    manual = []
    if "def load_data" in source:
        manual.append(
            'replace the body of `load_data()` with `return paths.load_dataset("<short_name>").read("<table>")`'
            " (the first --dep is the dataset), then delete `DATA_FILE`"
        )
    if re.search(r"^SOURCE = ", source, re.MULTILINE):
        manual.append(
            'in `run()`, replace `source = SOURCE` by `source_citation(tb["<column>"])` — after `load_data()`, '
            "inside `run()`, never at module level where `tb` does not exist yet — add `source_citation` to "
            "the etl.viz.static import, then delete the `SOURCE` constant"
        )
    if "# PROMOTE" in source:
        manual.append("drop the `# PROMOTE` comments once each edit is done")
    return source, manual


def format_import(module: str, names: list[str]) -> str:
    line = f"from {module} import {', '.join(names)}"
    if len(line) <= MAX_LINE:
        return line
    return f"from {module} import (\n" + "".join(f"    {n},\n" for n in names) + ")"


def dag_block(uri: str, deps: list[str], comment: str) -> str:
    return "\n".join([f"  # {comment}", f"  {uri}:", *[f"    - {d}" for d in deps]]) + "\n"


if __name__ == "__main__":
    sys.exit(main())
