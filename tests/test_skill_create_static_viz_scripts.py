"""Guards for the `create-static-viz` sketch scripts.

Nothing under `.claude/` is covered by CI (the pre-commit hook does not lint it and no other test
collects it), so these run the two scripts the way a session does. `new_sketch.py` scaffolds into a
temp dir from a three-row CSV and the sketch is rendered as a subprocess with this interpreter;
`promote_sketch.py` runs against a temp git checkout seeded with a DAG file. The mutation cases pin the
refusals: a sketch whose markers are gone must not be promoted quietly.
"""

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
