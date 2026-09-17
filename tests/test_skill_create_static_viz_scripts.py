"""Guards for the `create-static-viz` sketch scripts.

Nothing under `.claude/` is covered by CI (the pre-commit hook does not lint it and no other test
collects it), so these run the two scripts the way a session does. `new_sketch.py` scaffolds into a
temp dir from a three-row CSV and the sketch is rendered as a subprocess with this interpreter;
`promote_sketch.py` runs against a temp git checkout seeded with a DAG file. The mutation cases pin the
refusals: a sketch whose markers are gone must not be promoted quietly.
"""

import ast
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

SCRIPTS = Path(".claude/skills/create-static-viz/scripts")


def _locate(relative: Path) -> Path:
    """Walk up from this file to the repo root, so the test works from a git worktree too."""
    for directory in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
        candidate = directory / relative
        if candidate.is_file():
            return candidate
    raise AssertionError(f"could not locate {relative} from {__file__}")


NEW_SKETCH = _locate(SCRIPTS / "new_sketch.py")
PROMOTE = _locate(SCRIPTS / "promote_sketch.py")
VERIFY = _locate(SCRIPTS / "verify_static_viz.py")
RUFF = Path(sys.executable).with_name("ruff")

STEP = "viz://static/demo/2026-09-17/demo"
DEPS = ["data://garden/demo/2026-09-17/demo", "data://garden/demography/2024-07-15/population"]
SEED_DAG = "steps:\n  # Existing\n  viz://static/x/2026-01-01/existing:\n    - data://garden/x/2026-01-01/existing\n"


def run(*args, cwd=None):
    return subprocess.run([sys.executable, *map(str, args)], capture_output=True, text=True, cwd=cwd, check=False)


def frame() -> pd.DataFrame:
    return pd.DataFrame({"country": ["France"] * 3, "year": [2000, 2010, 2020], "value": [1.5, 2.5, 4.0]})


@pytest.fixture(scope="module")
def sketch(tmp_path_factory) -> Path:
    """One scaffold for the module: new_sketch.py imports etl.viz.static, which costs a couple of seconds."""
    base = tmp_path_factory.mktemp("sketch")
    csv = base / "demo.csv"
    frame().to_csv(csv, index=False)
    result = run(
        NEW_SKETCH,
        *("--slug", "demo", "--data", csv, "--template", "horizontal", "--template", "mobile"),
        *("--out-root", base / "sketches", "--source", "Demo (2026)", "--author", "A. Tester"),
    )
    assert result.returncode == 0, result.stderr
    return base / "sketches" / "demo"


def make_repo(tmp_path: Path, branch: str) -> Path:
    root = tmp_path / "repo"
    (root / "dag").mkdir(parents=True)
    (root / "etl/steps/viz/static").mkdir(parents=True)
    (root / "dag/static_viz.yml").write_text(SEED_DAG)
    subprocess.run(["git", "init", "-q", f"--initial-branch={branch}"], cwd=root, check=True)
    return root


def promote(sketch_dir: Path, repo: Path, *extra: str):
    return run(PROMOTE, sketch_dir, "--step", STEP, "--dep", DEPS[0], "--dep", DEPS[1], "--repo-root", repo, *extra)


# --- new_sketch.py ---------------------------------------------------------------------------------


def test_scaffold_renders_and_passes_the_verifier(sketch):
    result = run(sketch / "sketch.py")
    assert result.returncode == 0, result.stderr
    for name in ("demo", "demo_mobile"):
        assert (sketch / f"{name}.svg").is_file() and (sketch / f"{name}.png").is_file(), name

    verify = run(VERIFY, sketch, "--template", "horizontal", "--expect-gid", "line__placeholder")
    assert verify.returncode == 0, verify.stdout + verify.stderr
    assert "frame matches horizontal" in verify.stdout
    assert "frame matches mobile" in verify.stdout, "the _mobile suffix must pick the mobile template"
    assert "font-family: 'Lato'" in (sketch / "demo.svg").read_text(), "Lato first, so Figma renders the template face"


@pytest.mark.skipif(not RUFF.exists(), reason="ruff not installed next to the interpreter")
def test_scaffold_is_ruff_clean(sketch):
    # It lands under etl/steps on promotion, where `make check` lints it.
    for args in (["check"], ["format", "--check"]):
        result = subprocess.run([str(RUFF), *args, str(sketch / "sketch.py")], capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr


def test_scaffold_carries_the_promotion_markers(sketch):
    source = (sketch / "sketch.py").read_text()
    assert source.count("paths = SketchPaths(__file__)") == 1
    assert 'SOURCE = "Demo (2026)"' in source and "source_citation" in source
    assert source.index("def run()") < source.index("source = SOURCE") < source.index("def load_data()"), (
        "the citation swap happens inside run(), where tb exists — not at module level"
    )
    assert "Figma handoff" in source, "the docstring section the Figma sketch mode fills in"
    assert source.index("def run()") < source.index("def load_data()"), "helpers go below run()"
    assert '"demo": {' in source and '"demo_mobile": {' in source
    assert '"template": "horizontal"' in source and '"template": "mobile"' in source
    assert "A. Tester" in source


@pytest.mark.parametrize(
    "slug,template,data_name",
    [
        ("Bad-Slug", "horizontal", "demo.csv"),  # not snake_case
        ("demo_mobile", "horizontal", "demo.csv"),  # ends in a verifier template hint
        ("demo", "landscape", "demo.csv"),  # not a template
        ("demo", "horizontal", "missing.csv"),  # no such file
        ("demo", "horizontal", "demo.txt"),  # not a data format
    ],
)
def test_rejects_bad_inputs(tmp_path, slug, template, data_name):
    frame().to_csv(tmp_path / "demo.csv", index=False)
    (tmp_path / "demo.txt").write_text("not data")
    out_root = tmp_path / "sketches"
    result = run(
        NEW_SKETCH, "--slug", slug, "--data", tmp_path / data_name, "--template", template, "--out-root", out_root
    )
    assert result.returncode != 0
    assert not out_root.exists(), "nothing may be created when the arguments are refused"


@pytest.mark.parametrize("suffix", [".xlsx", ".parquet"])
def test_excel_and_parquet_inputs_render(tmp_path, suffix):
    data = tmp_path / f"demo{suffix}"
    (frame().to_excel if suffix == ".xlsx" else frame().to_parquet)(data, index=False)
    out_root = tmp_path / "sketches"
    scaffold = run(NEW_SKETCH, "--slug", "demo", "--data", data, "--template", "mobile-square", "--out-root", out_root)
    assert scaffold.returncode == 0, scaffold.stderr
    rendered = run(out_root / "demo" / "sketch.py")
    assert rendered.returncode == 0, rendered.stderr
    assert (out_root / "demo" / "demo.svg").is_file()


def test_quoted_arguments_become_valid_literals(tmp_path):
    """Title, source, author and filename are the person's own words: quotes and backslashes must survive."""
    title, source, author = 'Share reporting "good" health', 'Producer\'s "survey" (2026)', 'J. "Jo" O\'Neil \\ Co'
    data = tmp_path / "demo's data.csv"
    frame().to_csv(data, index=False)
    out_root = tmp_path / "sketches"
    scaffold = run(
        NEW_SKETCH,
        *("--slug", "demo", "--data", data, "--template", "horizontal", "--out-root", out_root),
        *("--title", title, "--source", source, "--author", author),
    )
    assert scaffold.returncode == 0, scaffold.stderr
    sketch_dir = out_root / "demo"

    module = ast.parse((sketch_dir / "sketch.py").read_text())  # a SyntaxError here is the bug
    constants = {
        target.id: node.value.value
        for node in module.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert (constants["TITLE"], constants["SOURCE"], constants["AUTHOR"]) == (title, source, author)
    rendered = run(sketch_dir / "sketch.py")
    assert rendered.returncode == 0, rendered.stderr  # DATA_FILE resolved to the quoted filename

    repo = make_repo(tmp_path, "feature")
    result = promote(sketch_dir, repo)
    assert result.returncode == 0, result.stderr
    assert f"# {title}" in (repo / "dag/static_viz.yml").read_text(), "the DAG comment is the title, unescaped"


def add_layout_variant(sketch_path: Path, name: str) -> None:
    """Copy the first LAYOUTS entry under `name`, the way section 4 says to try a variant."""
    source = sketch_path.read_text()
    first = re.search(r'^(    "[a-z0-9_]+": \{\n(?:.*?\n)*?    \},\n)', source, re.MULTILINE)
    assert first, "could not find a LAYOUTS entry to copy"
    variant = re.sub(r'^    "[a-z0-9_]+":', f'    "{name}":', first.group(1), count=1)
    sketch_path.write_text(source.replace(first.group(1), first.group(1) + variant, 1))


def test_force_clears_stale_frames_and_keeps_other_files(tmp_path):
    csv = tmp_path / "demo.csv"
    frame().to_csv(csv, index=False)
    out_root = tmp_path / "sketches"
    common = ("--slug", "demo", "--data", csv, "--out-root", out_root)
    first = run(NEW_SKETCH, *common, "--template", "horizontal", "--template", "mobile")
    assert first.returncode == 0, first.stderr
    sketch_dir = out_root / "demo"
    # A section-4 variant: another LAYOUTS key, added by hand, rendering a pair of its own.
    add_layout_variant(sketch_dir / "sketch.py", "demo_narrow")
    assert run(sketch_dir / "sketch.py").returncode == 0
    assert (sketch_dir / "demo_mobile.svg").is_file() and (sketch_dir / "demo_narrow.svg").is_file()
    theirs = {
        "reference.png": b"an old chart",
        "hand_edit.svg": b"<svg/>",
        "hand_edit.png": b"png",
        "notes.md": b"keep",
    }
    for name, content in theirs.items():
        (sketch_dir / name).write_bytes(content)

    again = run(NEW_SKETCH, *common, "--template", "mobile-square", "--force")
    assert again.returncode == 0, again.stderr
    assert "Cleared stale frames" in again.stdout
    for stem in ("demo", "demo_mobile", "demo_narrow"):
        assert not (sketch_dir / f"{stem}.svg").exists() and not (sketch_dir / f"{stem}.png").exists(), stem
    for name, content in theirs.items():
        assert (sketch_dir / name).read_bytes() == content, f"{name} is not the scaffold's to delete"

    assert str(sketch_dir / "demo") in again.stdout, "the printed verify command names a frame stem, not the dir"
    assert run(sketch_dir / "sketch.py").returncode == 0

    # Per frame stem, with the hand-made SVG still sitting there — the documented workflow.
    verify = run(VERIFY, sketch_dir / "demo", "--template", "mobile-square", "--expect-gid", "line__placeholder")
    assert verify.returncode == 0, verify.stdout + verify.stderr
    assert verify.stdout.count("OK   ") == 1 and "hand_edit" not in verify.stdout, "only this frame is checked"
    # And the reason the stem form is what we print: the directory form trips over files we preserved.
    whole_dir = run(VERIFY, sketch_dir, "--template", "mobile-square", "--expect-gid", "line__placeholder")
    assert whole_dir.returncode != 0 and "hand_edit" in whole_dir.stdout + whole_dir.stderr


@pytest.mark.parametrize("years", [["2018/19", "2019/20", "2020/21"], ["2000-01-01", "2010-01-01", "2020-01-01"]])
def test_source_native_year_labels_still_render(tmp_path, years):
    """A `year` column may carry the source's own period labels; converting them is a garden job.

    The scaffold's promise is that it renders as scaffolded, so it must fall back to plain ticks
    instead of raising when the axis is not whole years.
    """
    data = tmp_path / "periods.csv"
    pd.DataFrame({"country": ["France"] * 3, "year": years, "value": [1.5, 2.5, 4.0]}).to_csv(data, index=False)
    out_root = tmp_path / "sketches"
    scaffold = run(NEW_SKETCH, "--slug", "periods", "--data", data, "--template", "horizontal", "--out-root", out_root)
    assert scaffold.returncode == 0, scaffold.stderr

    rendered = run(out_root / "periods" / "sketch.py")
    assert rendered.returncode == 0, rendered.stderr
    verify = run(VERIFY, out_root / "periods", "--template", "horizontal", "--expect-gid", "line__placeholder")
    assert verify.returncode == 0, verify.stdout + verify.stderr


def test_first_scaffold_refuses_to_take_a_name_a_file_already_holds(tmp_path):
    """Nothing in the directory is the scaffold's until it has written a sketch there.

    The old chart being refreshed is often saved in first, and it can carry the slug's own name.
    Leaving it in place is not enough: the frames are written under that name, so the collision has
    to be settled before a render command is offered — the directory is gitignored.
    """
    out_root = tmp_path / "sketches"
    sketch_dir = out_root / "demo"
    sketch_dir.mkdir(parents=True)
    csv = sketch_dir / "demo.csv"
    frame().to_csv(csv, index=False)
    theirs = {"demo.svg": b"<svg>the old chart</svg>", "demo.png": b"the old chart, as a png"}
    for name, content in theirs.items():
        (sketch_dir / name).write_bytes(content)
    common = ("--slug", "demo", "--data", csv, "--template", "horizontal", "--out-root", out_root)

    refused = run(NEW_SKETCH, *common)
    assert refused.returncode == 2
    assert "demo.svg" in refused.stderr and "demo.png" in refused.stderr and "--force" in refused.stderr
    assert not (sketch_dir / "sketch.py").exists(), "a refusal writes nothing"
    for name, content in theirs.items():
        assert (sketch_dir / name).read_bytes() == content

    # Renamed out of the way, the scaffold proceeds — and the old chart survives the render too.
    for name in theirs:
        (sketch_dir / name).rename(sketch_dir / f"old_{name}")
    assert run(NEW_SKETCH, *common).returncode == 0
    assert run(sketch_dir / "sketch.py").returncode == 0
    for name, content in theirs.items():
        assert (sketch_dir / f"old_{name}").read_bytes() == content, "renamed, so never the scaffold's"
    assert (sketch_dir / "demo.svg").is_file(), "the frame is written under the slug's own name"


def test_first_scaffold_takes_the_names_with_force(tmp_path):
    """--force is how the person says the collision is theirs to accept."""
    out_root = tmp_path / "sketches"
    sketch_dir = out_root / "demo"
    sketch_dir.mkdir(parents=True)
    csv = sketch_dir / "demo.csv"
    frame().to_csv(csv, index=False)
    (sketch_dir / "demo.svg").write_bytes(b"<svg>the old chart</svg>")
    (sketch_dir / "keep_me.svg").write_bytes(b"<svg>not a frame name</svg>")

    forced = run(
        NEW_SKETCH, "--slug", "demo", "--data", csv, "--template", "horizontal", "--out-root", out_root, "--force"
    )
    assert forced.returncode == 0, forced.stderr
    assert (sketch_dir / "sketch.py").is_file()
    assert (sketch_dir / "keep_me.svg").read_bytes() == b"<svg>not a frame name</svg>", "still not ours"


def test_first_scaffold_refuses_to_replace_a_different_data_file(tmp_path):
    """`--data` says which file to copy in, not which file at the destination may be replaced."""
    out_root = tmp_path / "sketches"
    sketch_dir = out_root / "demo"
    sketch_dir.mkdir(parents=True)
    theirs = b"country,year,value\nFrance,1900,42.0\n"
    (sketch_dir / "data.csv").write_bytes(theirs)
    incoming = tmp_path / "elsewhere" / "data.csv"
    incoming.parent.mkdir()
    frame().to_csv(incoming, index=False)
    common = ("--slug", "demo", "--data", incoming, "--template", "horizontal", "--out-root", out_root)

    refused = run(NEW_SKETCH, *common)
    assert refused.returncode == 2
    assert "data.csv" in refused.stderr and "--force" in refused.stderr
    assert (sketch_dir / "data.csv").read_bytes() == theirs
    assert not (sketch_dir / "sketch.py").exists(), "a refusal writes nothing"

    # --force is how the replacement is accepted; the sketch then renders from the incoming file.
    forced = run(NEW_SKETCH, *common, "--force")
    assert forced.returncode == 0, forced.stderr
    assert (sketch_dir / "data.csv").read_bytes() != theirs
    assert run(sketch_dir / "sketch.py").returncode == 0
    assert (sketch_dir / "demo.svg").is_file()


def test_data_already_in_the_sketch_dir_is_left_in_place(tmp_path):
    """SKETCHING.md section 0 pulls the data into the sketch dir before scaffolding: no --force, no self-copy."""
    out_root = tmp_path / "sketches"
    sketch_dir = out_root / "demo"
    sketch_dir.mkdir(parents=True)
    data = sketch_dir / "data.csv"
    frame().to_csv(data, index=False)
    before = data.read_bytes()

    first = run(NEW_SKETCH, "--slug", "demo", "--data", data, "--template", "horizontal", "--out-root", out_root)
    assert first.returncode == 0, first.stderr
    assert data.read_bytes() == before and (sketch_dir / "sketch.py").is_file()
    assert run(sketch_dir / "sketch.py").returncode == 0

    second = run(NEW_SKETCH, "--slug", "demo", "--data", data, "--template", "horizontal", "--out-root", out_root)
    assert second.returncode == 2 and "sketch.py exists" in second.stderr, (
        "an existing sketch.py is what --force guards"
    )


# --- promote_sketch.py -----------------------------------------------------------------------------


def test_promote_writes_step_and_dag_entry(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    result = promote(sketch, repo)
    assert result.returncode == 0, result.stderr

    step = repo / "etl/steps/viz/static/demo/2026-09-17/demo.py"
    source = step.read_text()
    assert source.count("paths = PathFinder(__file__)") == 1
    assert "SketchPaths" not in source
    assert source.index("from etl.helpers import PathFinder") < source.index("from etl.viz.static import")
    assert "Figma handoff" in source, "the docstring travels with the file"
    if RUFF.exists():
        lint = subprocess.run([str(RUFF), "check", "--select", "I,F", str(step)], capture_output=True, text=True)
        assert lint.returncode == 0, lint.stdout

    steps = yaml.safe_load((repo / "dag/static_viz.yml").read_text())["steps"]
    assert steps["viz://static/x/2026-01-01/existing"] == ["data://garden/x/2026-01-01/existing"]
    assert steps[STEP] == DEPS, "both deps, in the order given"
    assert "# Demo" in (repo / "dag/static_viz.yml").read_text(), "the comment defaults to the sketch's TITLE"

    for word in ("load_data", "SOURCE", "source_citation", "finalize"):
        assert word in result.stdout, f"the manual checklist must name {word}"


def test_promote_refuses_master(tmp_path, sketch):
    repo = make_repo(tmp_path, "master")
    result = promote(sketch, repo)
    assert result.returncode == 2 and "master" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


def test_dry_run_writes_nothing(tmp_path, sketch):
    repo = make_repo(tmp_path, "master")  # the guard is bypassed too: a dry run is safe anywhere
    result = promote(sketch, repo, "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout and STEP in result.stdout
    assert not (repo / "etl/steps/viz/static/demo").exists()
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


def mutated(sketch: Path, tmp_path: Path, old: str, new: str) -> Path:
    copy = tmp_path / "mutant"
    shutil.copytree(sketch, copy)
    source = (copy / "sketch.py").read_text()
    assert source.count(old) == 1, old
    (copy / "sketch.py").write_text(source.replace(old, new))
    return copy


def test_promote_refuses_a_rewritten_paths_line(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    mutant = mutated(sketch, tmp_path, "paths = SketchPaths(__file__)", "paths = SketchPaths(Path(__file__))")
    result = promote(mutant, repo)
    assert result.returncode == 2 and "exactly one" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


def test_promote_refuses_an_import_without_sketchpaths(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    mutant = mutated(sketch, tmp_path, "TEMPLATES, SketchPaths, apply_svg_rcparams", "TEMPLATES, apply_svg_rcparams")
    result = promote(mutant, repo)
    assert result.returncode == 2 and "not a scaffold" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()


def test_promote_refuses_a_step_already_in_the_dag(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    (repo / "dag/static_viz.yml").write_text(SEED_DAG + f"\n  # Taken\n  {STEP}:\n    - {DEPS[0]}\n")
    result = promote(sketch, repo)
    assert result.returncode == 2 and "already in" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()


def test_promote_refuses_a_short_name_that_differs_from_the_slug(tmp_path, sketch):
    """The frames and LAYOUTS keys carry the slug; only the file would be renamed, leaving two identities."""
    repo = make_repo(tmp_path, "feature")
    step = "viz://static/demo/2026-09-17/other"
    result = run(PROMOTE, sketch, "--step", step, "--dep", DEPS[0], "--repo-root", repo)
    assert result.returncode == 2 and "slug is 'demo'" in result.stderr and "'other'" in result.stderr
    assert not list((repo / "etl/steps/viz/static").iterdir()), "nothing may be written on a refusal"
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG
