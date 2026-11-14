"""Guards for the `create-static-viz` sketch scripts.

Nothing under `.claude/` is covered by CI (the pre-commit hook does not lint it and no other test
collects it), so these run the two scripts the way a session does. `new_sketch.py` scaffolds into a
temp dir from a three-row CSV and the sketch is rendered as a subprocess with this interpreter;
`finalize_sketch.py` runs against a temp git checkout seeded with a DAG file. The mutation cases pin the
refusals: a sketch whose markers are gone must not be finalized quietly.
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
FINALIZE = _locate(SCRIPTS / "finalize_sketch.py")
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


def finalize(sketch_dir: Path, repo: Path, *extra: str):
    return run(FINALIZE, sketch_dir, "--step", STEP, "--dep", DEPS[0], "--dep", DEPS[1], "--repo-root", repo, *extra)


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
    # It lands under etl/steps when finalized, where `make check` lints it.
    for args in (["check"], ["format", "--check"]):
        result = subprocess.run([str(RUFF), *args, str(sketch / "sketch.py")], capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr


def test_scaffold_names_every_mark(sketch):
    # A finalized sketch is a new step, and the skill asks new steps to pass the strict naming check.
    verify = run(VERIFY, sketch, "--template", "horizontal", "--require-named-marks")
    assert verify.returncode == 0, verify.stdout + verify.stderr


def test_scaffold_draws_tick_marks_at_grapher_size(sketch):
    """Grapher's mark is 5px x 1px in #999: 3.6pt x 0.72pt, as matplotlib takes both in points."""
    if not (sketch / "demo.svg").is_file():
        assert run(sketch / "sketch.py").returncode == 0
    svg = (sketch / "demo.svg").read_text()
    mark = re.search(r'<g id="axes__x-tick-\d+">\s*<defs>\s*<path [^>]*d="M 0 0\s+L 0 ([0-9.]+)\s*"[^>]*>', svg)
    assert mark, "no x tick mark found in the scaffold's SVG"
    width = re.search(r"stroke-width: ([0-9.]+)", mark.group())
    stroke = re.search(r"stroke: (#[0-9a-f]+)", mark.group())
    got = (float(mark.group(1)), float(width.group(1)) if width else None, stroke.group(1) if stroke else None)
    assert got == (3.6, 0.72, "#999999"), mark.group()
    assert re.search(r'id="axes__x-tick-label-\d+">\s*<text style="[^"]*fill: #5b5b5b', svg), (
        "labels keep the text color"
    )


# --- verify_static_viz.py: named marks --------------------------------------------------------------

SVG_HEAD = (
    '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="850" height="638">'
)


def write_frame(tmp_path: Path, body: str) -> Path:
    """A horizontal-template SVG and its sibling PNG, holding a named title and whatever `body` adds."""
    from PIL import Image

    stem = tmp_path / "frame"
    title = '<g id="title"><text style="fill: #2d2e2d">Title</text></g>'
    stem.with_suffix(".svg").write_text(f"{SVG_HEAD}{title}{body}</svg>")
    Image.new("RGB", (850, 638), "white").save(stem.with_suffix(".png"))
    return stem


def test_unnamed_furniture_fails_only_when_required(tmp_path):
    body = (
        '<g id="row__bar"><path d="M0 0h10v10z" style="fill: #00295b"/></g>'
        '<g id="line2d_1"><path d="M0 50h400" style="fill: none; stroke: #dddddd"/></g>'
    )
    stem = write_frame(tmp_path, body)
    lenient = run(VERIFY, stem, "--template", "horizontal")
    assert lenient.returncode == 0 and "1 unnamed marks" in lenient.stdout, lenient.stdout
    strict = run(VERIFY, stem, "--template", "horizontal", "--require-named-marks")
    assert strict.returncode == 1, strict.stdout
    assert "1 painted mark(s)" in strict.stdout and "line2d_1 <path>" in strict.stdout


def test_named_marks_ignore_what_paints_nothing_and_defs(tmp_path):
    body = (
        '<defs><path id="m0123456789" d="M0 0v4" style="stroke: #000000"/></defs>'
        '<g id="patch_1"><path d="M0 0h850v638z" style="fill: none"/></g>'
        '<g id="axis__x-ticks"><use xlink:href="#m0123456789" style="stroke: #000000"/></g>'
        '<g id="line2d_2"><path d="M0 0h10" style="fill: none; stroke: #000000; stroke-opacity: 0"/></g>'
    )
    stem = write_frame(tmp_path, body)
    strict = run(VERIFY, stem, "--template", "horizontal", "--require-named-marks")
    assert strict.returncode == 0, strict.stdout


def test_an_unnamed_raster_image_counts_as_a_mark(tmp_path):
    # imshow / figimage emit <image>, which a designer styles like any other layer.
    body = '<g id="image1"><image width="10" height="10" xlink:href="data:image/png;base64,AA=="/></g>'
    stem = write_frame(tmp_path, body)
    strict = run(VERIFY, stem, "--template", "horizontal", "--require-named-marks")
    assert strict.returncode == 1 and "image1 <image>" in strict.stdout, strict.stdout


def test_a_shared_gid_fails_only_when_required(tmp_path):
    body = (
        '<g id="series__same"><path d="M0 0h10" style="fill: none; stroke: #00295b"/></g>'
        '<g id="series__same"><path d="M0 5h10" style="fill: none; stroke: #b13507"/></g>'
    )
    stem = write_frame(tmp_path, body)
    assert run(VERIFY, stem, "--template", "horizontal").returncode == 0
    strict = run(VERIFY, stem, "--template", "horizontal", "--require-named-marks")
    assert strict.returncode == 1 and "series__same" in strict.stdout, strict.stdout


# --- verify_static_viz.py: declared frame size -------------------------------------------------------


def write_sized_frame(tmp_path: Path, width: int, height: int, unit: str = "") -> Path:
    """A named SVG and its sibling PNG at a given frame size, in `unit` on the SVG root."""
    from PIL import Image

    stem = tmp_path / "frame"
    w, h = (round(width * 0.72, 2), round(height * 0.72, 2)) if unit == "pt" else (width, height)
    head = f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}{unit}" height="{h}{unit}">'
    stem.with_suffix(".svg").write_text(f'{head}<g id="title"><text>Title</text></g></svg>')
    Image.new("RGB", (width * 3, height * 3), "white").save(stem.with_suffix(".png"))
    return stem


def test_a_declared_size_replaces_the_template_ratio(tmp_path):
    # A layout trimmed off its template on purpose fails the template check, and passes once declared.
    stem = write_sized_frame(tmp_path, 850, 615)
    template_only = run(VERIFY, stem, "--template", "horizontal")
    assert template_only.returncode == 1 and "--size" in template_only.stdout, template_only.stdout
    declared = run(VERIFY, stem, "--template", "horizontal", "--size", "frame=850x615")
    assert declared.returncode == 0 and "frame matches declared 850x615" in declared.stdout, declared.stdout


def test_a_declared_size_still_catches_a_crop(tmp_path):
    stem = write_sized_frame(tmp_path, 850, 600)
    result = run(VERIFY, stem, "--size", "frame=850x615")
    assert result.returncode == 1 and "declared size is 850x615" in result.stdout, result.stdout


def test_a_canvas_at_the_right_ratio_but_twice_the_scale_fails(tmp_path):
    # matplotlib writes points, so an 850px frame is 612pt; at 1224pt every font would halve in Figma.
    assert run(VERIFY, write_sized_frame(tmp_path, 850, 615, "pt"), "--size", "frame=850x615").returncode == 0
    doubled = write_sized_frame(tmp_path, 1700, 1230, "pt")
    for args in (["--size", "frame=850x615"], ["--template", "horizontal", "--size", "frame=850x615"]):
        result = run(VERIFY, doubled, *args)
        assert result.returncode == 1 and "the scale is 2x" in result.stdout, result.stdout


@pytest.mark.parametrize("size", ["frame=850", "frame=850x0", "850x615", "frame=wxh"])
def test_a_malformed_size_is_refused(tmp_path, size):
    stem = write_sized_frame(tmp_path, 850, 615)
    assert run(VERIFY, stem, "--size", size).returncode == 2


def test_a_size_for_a_missing_output_is_refused(tmp_path):
    # A typo in the stem would otherwise fall back to the template check without a word.
    stem = write_sized_frame(tmp_path, 850, 615)
    result = run(VERIFY, tmp_path, "--size", "fram=850x615")
    assert result.returncode == 2 and "fram" in result.stderr, result.stderr
    assert stem.with_suffix(".svg").exists()


def test_scaffold_carries_the_finalize_markers(sketch):
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
    result = finalize(sketch_dir, repo)
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


# --- finalize_sketch.py ----------------------------------------------------------------------------


def test_finalize_writes_step_and_dag_entry(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    result = finalize(sketch, repo)
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
    assert "finalize mode on the sketch frame" in result.stdout, "both halves: the checklist ends in the Figma checks"


def test_finalize_etl_only_leaves_the_figma_checks_out(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    result = finalize(sketch, repo, "--etl-only")
    assert result.returncode == 0, result.stderr
    assert "finalize mode on the sketch frame" not in result.stdout, "the person opted out of the Figma half"
    assert "without the Figma checks" in result.stdout and "still to run" in result.stdout


def test_finalize_refuses_master(tmp_path, sketch):
    repo = make_repo(tmp_path, "master")
    result = finalize(sketch, repo)
    assert result.returncode == 2 and "master" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


def test_dry_run_writes_nothing(tmp_path, sketch):
    repo = make_repo(tmp_path, "master")  # the guard is bypassed too: a dry run is safe anywhere
    result = finalize(sketch, repo, "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout and STEP in result.stdout
    assert not (repo / "etl/steps/viz/static/demo").exists()
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


def test_finalize_accepts_a_sketch_with_the_older_promote_markers(tmp_path, sketch):
    # Sketches scaffolded before the mode was renamed carry `# PROMOTE` comments instead of `# FINALIZE`.
    repo = make_repo(tmp_path, "feature")
    copy = tmp_path / "older"
    shutil.copytree(sketch, copy)
    source = (copy / "sketch.py").read_text()
    assert "# FINALIZE" in source
    (copy / "sketch.py").write_text(source.replace("# FINALIZE", "# PROMOTE"))
    result = finalize(copy, repo)
    assert result.returncode == 0, result.stderr
    assert "drop the `# PROMOTE` comments" in result.stdout


def test_old_promote_script_path_still_finalizes(tmp_path, sketch):
    # Older sketches' docstrings name `promote_sketch.py`; that path forwards to `finalize_sketch.py`.
    repo = make_repo(tmp_path, "feature")
    old = _locate(SCRIPTS / "promote_sketch.py")
    result = run(old, sketch, "--step", STEP, "--dep", DEPS[0], "--dep", DEPS[1], "--repo-root", repo)
    assert result.returncode == 0, result.stderr
    assert (repo / "etl/steps/viz/static/demo/2026-09-17/demo.py").is_file()


def mutated(sketch: Path, tmp_path: Path, old: str, new: str) -> Path:
    copy = tmp_path / "mutant"
    shutil.copytree(sketch, copy)
    source = (copy / "sketch.py").read_text()
    assert source.count(old) == 1, old
    (copy / "sketch.py").write_text(source.replace(old, new))
    return copy


def test_finalize_refuses_a_rewritten_paths_line(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    mutant = mutated(sketch, tmp_path, "paths = SketchPaths(__file__)", "paths = SketchPaths(Path(__file__))")
    result = finalize(mutant, repo)
    assert result.returncode == 2 and "exactly one" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


def test_finalize_refuses_an_import_without_sketchpaths(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    mutant = mutated(sketch, tmp_path, "TEMPLATES, SketchPaths, apply_svg_rcparams", "TEMPLATES, apply_svg_rcparams")
    result = finalize(mutant, repo)
    assert result.returncode == 2 and "not a scaffold" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()


def test_finalize_refuses_a_step_already_in_the_dag(tmp_path, sketch):
    repo = make_repo(tmp_path, "feature")
    (repo / "dag/static_viz.yml").write_text(SEED_DAG + f"\n  # Taken\n  {STEP}:\n    - {DEPS[0]}\n")
    result = finalize(sketch, repo)
    assert result.returncode == 2 and "already in" in result.stderr
    assert not (repo / "etl/steps/viz/static/demo").exists()


@pytest.mark.parametrize("via", ["title", "comment"])
def test_a_multiline_comment_cannot_write_into_the_dag(tmp_path, via):
    """Only a comment's first line carries the `#`; an indented continuation would become a step key."""
    base = tmp_path / "src"
    base.mkdir()
    csv = base / "demo.csv"
    frame().to_csv(csv, index=False)
    multiline = "Demo\n  injected_key: [ohno]"
    out_root = tmp_path / "sketches"
    scaffold = run(
        NEW_SKETCH,
        *("--slug", "demo", "--data", csv, "--template", "horizontal", "--out-root", out_root),
        *(("--title", multiline) if via == "title" else ()),
    )
    assert scaffold.returncode == 0, scaffold.stderr

    repo = make_repo(tmp_path, "feature")
    extra = ("--comment", multiline) if via == "comment" else ()
    result = finalize(out_root / "demo", repo, *extra)
    assert result.returncode == 0, result.stderr

    dag_text = (repo / "dag/static_viz.yml").read_text()
    steps = yaml.safe_load(dag_text)["steps"]
    assert set(steps) == {"viz://static/x/2026-01-01/existing", STEP}, f"injected keys: {sorted(steps)}"
    assert "# Demo injected_key: [ohno]" in dag_text, "folded onto the comment's single line"
    assert [line for line in dag_text.splitlines() if "injected_key" in line] == ["  # Demo injected_key: [ohno]"], (
        "the text may appear only inside the comment"
    )


def test_finalize_refuses_a_short_name_that_differs_from_the_slug(tmp_path, sketch):
    """The frames and LAYOUTS keys carry the slug; only the file would be renamed, leaving two identities."""
    repo = make_repo(tmp_path, "feature")
    step = "viz://static/demo/2026-09-17/other"
    result = run(FINALIZE, sketch, "--step", step, "--dep", DEPS[0], "--repo-root", repo)
    assert result.returncode == 2 and "slug is 'demo'" in result.stderr and "'other'" in result.stderr
    assert not list((repo / "etl/steps/viz/static").iterdir()), "nothing may be written on a refusal"
    assert (repo / "dag/static_viz.yml").read_text() == SEED_DAG


# --- verify_static_viz.py: grapher's axes ------------------------------------------------------------


def write_axes_frame(
    tmp_path: Path,
    *,
    label_px: float = 15,
    y_anchor: str = "end",
    tick_marks: bool | str | None = True,
    x_values: tuple[int, ...] = (2000, 2010, 2020),
    x_format: str = "{}",
    x_labels: tuple[str, ...] | None = None,
    math: bool = False,
    second_axes: bool = False,
) -> Path:
    """A 540-square SVG in points, as matplotlib writes it, with a year axis and a y axis named by name_furniture.

    `tick_marks=None` omits the x-tick groups, as matplotlib does for a hidden mark (`bottom=False`), and
    `"transparent"` writes them at zero opacity, as it does for `tick1line.set_alpha(0)`.
    `x_format` turns each x tick value into its label text, or `x_labels` gives the texts outright.
    `math` writes every label in matplotlib's MathText shape (see `math_label`), the y labels as
    powers of ten. `second_axes` adds an `axes2` panel whose x axis counts (0, 50, 100) with tick marks
    drawn.
    """
    from PIL import Image

    stem = tmp_path / "frame_square"
    style = f"font-size: {label_px * 0.72:.2f}px; font-family: 'Lato'; text-anchor: middle"
    texts = x_labels or tuple(x_format.format(v) for v in x_values)

    def label(text: str, anchor_style: str, anchor: str = "middle") -> str:
        return math_label(text, label_px, anchor) if math else f'<text style="{anchor_style}">{text}</text>'

    mark = (
        '<g id="axes__x-tick-{v}"><g><use xlink:href="#m1" x="10" y="300" '
        'style="fill-opacity: 0; stroke: #000000; stroke-opacity: 0; stroke-width: 0.8"/></g></g>'
        if tick_marks == "transparent"
        else '<g id="axes__x-tick-{v}"><g><use xlink:href="#m1" x="10" y="300" style="stroke: #999999"/></g></g>'
        if tick_marks
        else '<g id="axes__x-tick-{v}"/>'
        if tick_marks is False
        else ""
    )
    xs = "".join(
        f'<g id="xtick_{i}">{mark.format(v=v)}<g id="axes__x-tick-label-{v}">{label(text, style)}</g></g>'
        for i, (v, text) in enumerate(zip(x_values, texts))
    )
    if second_axes:
        xs += "".join(
            f'<g id="xtick_{i + 10}"><g id="axes2__x-tick-{v}"><g><use xlink:href="#m1" x="10" y="300"/></g></g>'
            f'<g id="axes2__x-tick-label-{v}"><text style="{style}">{v}</text></g></g>'
            for i, v in enumerate((0, 50, 100))
        )
    ystyle = style.replace("text-anchor: middle", f"text-anchor: {y_anchor}")
    ys = "".join(
        f'<g id="ytick_{i}"><g id="axes__y-tick-{v}"/>'
        f'<g id="axes__y-tick-label-{v}">{label(f"10^{v}" if math else str(v), ystyle, y_anchor)}</g></g>'
        for i, v in enumerate((0, 10, 20))
    )
    head = (
        '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" '
        'width="388.8pt" height="388.8pt">'
    )
    defs = '<defs><path id="m1" d="M 0 0 L 0 5"/></defs>'
    stem.with_suffix(".svg").write_text(f'{head}{defs}<g id="title"><text>Title</text></g>{xs}{ys}</svg>')
    Image.new("RGB", (1620, 1620), "white").save(stem.with_suffix(".png"))
    return stem


def math_label(text: str, label_px: float, anchor: str = "middle") -> str:
    """A tick label as matplotlib writes MathText: a comment, a transform group, one tspan per glyph.

    It carries no text-anchor: matplotlib aligns it by moving the transform, so that its `anchor` edge
    (start, middle or end) lands on x=40, one shared x for every label of an axis. A `^` raises the
    rest of the text to a superscript, on its own baseline and at 70% of the size.
    """
    base, _, sup = text.partition("^")
    glyphs = [(c, label_px, "-0.06") for c in base] + [(c, label_px * 0.7, "-4.8") for c in sup]
    tspans = "".join(
        f'<tspan x="{6 * i}" y="{y}" style="font-size: {px * 0.72:.2f}px; font-family: \'Lato\'">{c}</tspan>'
        for i, (c, px, y) in enumerate(glyphs)
    )
    extent = 6 * (len(glyphs) - 1)
    left = 40 - {"start": 0, "middle": extent / 2, "end": extent}[anchor]
    return f'<!-- ${text}$ --><g transform="translate({left:g} 300)"><text>{tspans}</text></g>'


@pytest.mark.parametrize("kwargs", [{}, {"math": True}])
def test_grapher_axes_pass(tmp_path, kwargs):
    result = run(VERIFY, write_axes_frame(tmp_path, **kwargs), "--template", "mobile-square")
    assert result.returncode == 0, result.stdout


@pytest.mark.parametrize(
    "ha, label_pt, rotation, tick_alpha, message",
    [
        ("right", 13 * 0.72, 0, 1, "tick labels are 13px"),
        ("left", 15 * 0.72, 0, 1, "y-axis tick labels are anchored start"),
        ("center", 15 * 0.72, 0, 1, "y-axis tick labels are anchored middle"),
        ("left", 15 * 0.72, 45, 1, "the y-axis tick labels of axes are rotated"),
        ("right", 15 * 0.72, 0, 0, "the year axis of axes has no tick marks"),
    ],
)
def test_mathtext_labels_from_matplotlib_are_checked(tmp_path, ha, label_pt, rotation, tick_alpha, message):
    """A real log axis, whose labels matplotlib writes as MathText, is held to the size and alignment.

    The last case hides the year axis' marks with an alpha of 0, which leaves them in the SVG unpainted.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([2000, 2010, 2020], [1, 1e5, 1e12], gid="line__value")
    ax.set_yscale("log")
    # Exponents of one and two digits, so the labels differ in width and their alignment shows.
    ax.set_yticks([1, 1e5, 1e12])
    ax.minorticks_off()
    # Year labels written as grapher writes them, so the x axis reads as a year axis.
    ax.set_xticks([2000, 2010, 2020], ["2000", "2010", "2020"])
    # Only the MathText labels are off, so only reading them can fail the frame.
    ax.tick_params(axis="x", labelsize=15 * 0.72, length=5 * 0.72, width=0.72)
    ax.tick_params(axis="y", labelsize=label_pt)
    for tick_label in ax.get_yticklabels():
        tick_label.set_horizontalalignment(ha)
        tick_label.set_rotation(rotation)
    for tick in ax.xaxis.get_major_ticks():
        tick.tick1line.set_alpha(tick_alpha)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    assert "<!-- $" in (tmp_path / "frame_square.svg").read_text(), "matplotlib no longer writes MathText labels"
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    assert result.returncode == 1 and message in result.stdout, result.stdout
    assert result.stdout.count("\n  - ") == 1, f"only the {message!r} failure is expected: {result.stdout}"


@pytest.mark.parametrize(
    "ha, edge, message",
    [
        ("right", "grid", None),
        ("right", "spine", None),
        # Drawn y ticks carry their mark's <defs>, at its own origin, which is not the plot edge.
        ("right", "ticks", None),
        ("left", "ticks", "the y-axis tick labels of axes run past the plot's left edge"),
        ("left", "grid", "the y-axis tick labels of axes run past the plot's left edge"),
        ("center", "grid", "the y-axis tick labels of axes run past the plot's left edge"),
        ("left", "spine", "the y-axis tick labels of axes run past the plot's left edge"),
        ("right", None, "are MathText of one width with no y gridline"),
    ],
)
def test_mathtext_labels_of_one_width_are_judged_against_the_plot_edge(tmp_path, ha, edge, message):
    """A log axis of 10^1, 10^2, 10^3: labels of one width line up on every edge, so where they end is what is read.

    `edge` is what marks the plot's left edge in the SVG: grapher's horizontal gridlines, the left
    spine, drawn y tick marks, or nothing, when the alignment cannot be read at all.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([2000, 2010, 2020], [10, 100, 1000], gid="line__value")
    ax.set_yscale("log")
    ax.set_yticks([10, 100, 1000])
    ax.minorticks_off()
    ax.set_xticks([2000, 2010, 2020], ["2000", "2010", "2020"])
    ax.tick_params(axis="x", labelsize=15 * 0.72, length=5 * 0.72, width=0.72)
    # As the scaffold draws a y axis: no tick marks, and the left spine only, when `edge` asks for them.
    ax.tick_params(axis="y", labelsize=15 * 0.72, length=5 * 0.72 if edge == "ticks" else 0)
    ax.yaxis.grid(edge == "grid")
    ax.spines["left"].set_visible(edge == "spine")
    for tick_label in ax.get_yticklabels():
        tick_label.set_horizontalalignment(ha)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    if message is None:
        assert result.returncode == 0, result.stdout
    else:
        assert result.returncode == 1 and message in result.stdout, result.stdout
        assert result.stdout.count("\n  - ") == 1, f"only the {message!r} failure is expected: {result.stdout}"


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"label_px": 13}, "tick labels are 13px; on a 540-wide template they are 15px"),
        ({"y_anchor": "start"}, "y-axis tick labels are anchored start"),
        ({"tick_marks": False}, "the year axis of axes has no tick marks"),
        ({"tick_marks": None}, "the year axis of axes has no tick marks"),
        ({"tick_marks": "transparent"}, "the year axis of axes has no tick marks"),
        ({"tick_marks": False, "second_axes": True}, "the year axis of axes has no tick marks"),
        ({"tick_marks": False, "x_values": (-500, 0, 500, 1000, 1500, 2000)}, "the year axis of axes has no tick"),
        ({"tick_marks": False, "x_values": (2000, 2100, 2200, 2300)}, "the year axis of axes has no tick"),
        ({"tick_marks": False, "x_values": (0, 1000, 2000)}, "with a thousands separator (2,000)"),
        # Long-run axes group the digits of BCE years (world_population_long_run writes "12,000 BCE").
        (
            {
                "tick_marks": False,
                "x_values": (-12000, -2000, 0, 2000),
                "x_labels": ("12,000 BCE", "2,000 BCE", "0", "2000"),
            },
            "the year axis of axes has no tick marks",
        ),
        ({"tick_marks": False, "x_values": (1, 2), "x_labels": ("2,000 CE", "1,500 CE")}, "the year axis of axes"),
        # MathText labels nest their text, and still get every axis check.
        ({"math": True, "label_px": 13}, "tick labels are 13px; on a 540-wide template they are 15px"),
        ({"math": True, "tick_marks": False}, "the year axis of axes has no tick marks"),
        ({"math": True, "y_anchor": "start"}, "y-axis tick labels are anchored start"),
        ({"math": True, "y_anchor": "middle"}, "y-axis tick labels are anchored middle"),
    ],
)
def test_axes_that_differ_from_grapher_fail(tmp_path, kwargs, message):
    result = run(VERIFY, write_axes_frame(tmp_path, **kwargs), "--template", "mobile-square")
    assert result.returncode == 1 and message in result.stdout, result.stdout


@pytest.mark.parametrize(
    "kwargs",
    [
        {"x_values": (0, 25, 50, 75, 100)},
        {"x_values": (0, 1000, 2000), "x_format": "{:,}"},
        {"x_values": (0, 50, 100), "x_format": "{}%"},
        # A separator with no era suffix is a count, whatever the other labels are.
        {"x_values": (0, 1000, 2000), "x_labels": ("0", "1,000", "2,000")},
        # Powers of ten: the exponent's own baseline keeps 10^12 from reading as the year 1012.
        {"x_values": (3, 6, 12), "x_labels": ("10^3", "10^6", "10^12"), "math": True},
    ],
)
def test_a_non_year_axis_needs_no_tick_marks(tmp_path, kwargs):
    """Shares, small counts and counts labelled with a thousands separator are not years."""
    result = run(VERIFY, write_axes_frame(tmp_path, tick_marks=False, **kwargs), "--template", "mobile-square")
    assert result.returncode == 0, result.stdout


@pytest.mark.parametrize(
    "x_width, right_labels, y_rotation, y_ha, message",
    [
        (0, False, 0, None, "the year axis of axes has no tick marks"),
        (0.72, True, 0, None, "the y-axis tick labels of axes are drawn on the right"),
        # Rotated plain labels pass the anchor check, so only their transform shows the turn.
        (0.72, False, 45, "right", "the y-axis tick labels of axes are rotated"),
        (0.72, False, 45, None, "the y-axis tick labels of axes are rotated"),
    ],
)
def test_hidden_marks_and_right_side_labels_from_matplotlib_fail(
    tmp_path, x_width, right_labels, y_rotation, y_ha, message
):
    """Ways a real step loses grapher's axes: x ticks at width 0 (a strokeless <use>), y labels on the right or rotated."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([2000, 2010, 2020], [0, 25, 20], gid="line__value")
    ax.set_xticks([2000, 2010, 2020], ["2000", "2010", "2020"])
    ax.tick_params(labelsize=15 * 0.72)
    ax.tick_params(axis="x", length=5 * 0.72, width=x_width)
    if right_labels:
        ax.tick_params(axis="y", labelright=True, labelleft=False)
    ax.tick_params(axis="y", rotation=y_rotation)
    if y_ha:
        for tick_label in ax.get_yticklabels():
            tick_label.set_horizontalalignment(y_ha)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    assert result.returncode == 1 and message in result.stdout, result.stdout
    assert result.stdout.count("\n  - ") == 1, f"only the {message!r} failure is expected: {result.stdout}"


def test_an_axis_labelled_on_its_minor_ticks_is_checked(tmp_path):
    """Years written on minor ticks (FixedLocator/FixedFormatter), with the marks hidden, still fail."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FixedFormatter, FixedLocator, NullFormatter, NullLocator

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([2000, 2010, 2020], [0, 25, 20], gid="line__value")
    ax.xaxis.set_major_locator(NullLocator())
    ax.xaxis.set_major_formatter(NullFormatter())
    ax.xaxis.set_minor_locator(FixedLocator([2000, 2010, 2020]))
    ax.xaxis.set_minor_formatter(FixedFormatter(["2000", "2010", "2020"]))
    ax.tick_params(labelsize=15 * 0.72)
    ax.tick_params(axis="x", which="minor", labelsize=15 * 0.72, length=0)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    assert "x-minor-tick-label-" in (tmp_path / "frame_square.svg").read_text()
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    assert result.returncode == 1 and "the year axis of axes has no tick marks" in result.stdout, result.stdout


def test_a_year_axis_missing_one_of_its_marks_fails(tmp_path):
    """Every labelled year needs its own mark: hiding one tick1line is caught, not excused by the rest."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([2000, 2010, 2020], [0, 25, 20], gid="line__value")
    ax.set_xticks([2000, 2010, 2020], ["2000", "2010", "2020"])
    ax.tick_params(labelsize=15 * 0.72)
    ax.tick_params(axis="x", length=5 * 0.72, width=0.72)
    ax.xaxis.get_major_ticks()[1].tick1line.set_visible(False)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    assert result.returncode == 1 and "has no tick mark at 2010" in result.stdout, result.stdout


def test_x_labels_drawn_at_the_top_fail(tmp_path):
    """`labeltop=True, labelbottom=False` writes `x-tick-label2-*` groups: grapher's x labels sit below the plot."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([2000, 2010, 2020], [0, 25, 20], gid="line__value")
    ax.set_xticks([2000, 2010, 2020], ["2000", "2010", "2020"])
    ax.tick_params(labelsize=15 * 0.72)
    ax.tick_params(axis="x", labeltop=True, labelbottom=False, length=0)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    assert result.returncode == 1 and "are drawn at the top (labeltop)" in result.stdout, result.stdout


@pytest.mark.parametrize(
    "line_years, ticks, labels, message",
    [
        # The line runs to 2024 while the axis stops at a round 2020.
        ([2000, 2010, 2024], [2000, 2010, 2020], None, "ends its labels at 2020, but line__value run on to about 2024"),
        # A blank last label (a step thinning the ticks) does not count as closing the axis.
        ([2000, 2010, 2020], [2000, 2010, 2020], ["2000", "2010", ""], "ends its labels at 2010"),
        ([2000, 2010, 2024], [2000, 2010, 2024], None, None),
        # Grapher's pattern: the round step kept, the last year added after it.
        ([2000, 2010, 2024], [2000, 2005, 2010, 2015, 2020, 2024], None, None),
        # Within half a year of the last tick, as a bar centred on it reaches: not past it.
        ([2000, 2010, 2020.4], [2000, 2010, 2020], None, None),
    ],
)
def test_a_year_axis_closes_on_the_last_year_plotted(tmp_path, line_years, ticks, labels, message):
    """Grapher's time axis ends on the data's last year; a series line running past the last tick fails."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from etl.viz.static import SketchPaths, apply_svg_rcparams, export_frame

    apply_svg_rcparams()
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot(line_years, [0, 25, 20], gid="line__value")
    ax.set_xticks(ticks, labels or [str(t) for t in ticks])
    ax.set_xlim(min(ticks[0], line_years[0]), max(ticks[-1], line_years[-1]))
    ax.tick_params(labelsize=15 * 0.72)
    ax.tick_params(axis="x", length=5 * 0.72, width=0.72)
    # An inset on another scale, whose own line must not be judged against the main axis.
    inset = ax.inset_axes([0.6, 0.6, 0.3, 0.3])
    inset.plot([0, 1], [0, 1], gid="line__inset")
    inset.set_xticks([0, 1], ["0", "1"])
    inset.tick_params(labelsize=15 * 0.72)
    export_frame(SketchPaths(tmp_path / "sketch.py"), fig, "frame_square", template="mobile-square")
    plt.close(fig)
    result = run(VERIFY, tmp_path / "frame_square", "--template", "mobile-square")
    if message is None:
        assert result.returncode == 0, result.stdout
    else:
        assert result.returncode == 1 and message in result.stdout, result.stdout
        assert result.stdout.count("\n  - ") == 1, f"only the {message!r} failure is expected: {result.stdout}"
