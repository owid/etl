#!/usr/bin/env python3
"""Tests for measure_pixels.py, on synthetic renders built to reproduce CHECKS.md's failure modes.

Run:  .venv/bin/python .claude/skills/create-figma-chart/scripts/test_measure_pixels.py

The cases that matter are not "does it compute a distance" but the four traps the prose documents:
masking from the full render hides the overlap being tested for; classifying pixels by color
collects the wrong shape; an empty mask must be UNMEASURABLE rather than a clean pass; and a
sub-pixel stroke reads as a defect at 1:1 and clears the bar at 4x.

Anti-aliasing gets its own case, because it is the difference between these synthetic renders and
a real one, and because the obvious way to fake it is wrong. Fake AA by supersampling and
downscaling with **BOX** (area average). A ringing filter such as LANCZOS overshoots two or three
pixels past each edge, which inflates a `differs` mask enough that two shapes five pixels apart
have overlapping masks and every gap reads as 0 — an alarming result that says nothing about the
check. Two rigs were wrong before this one: that, and an arrow whose slant carried it into the
target so the nominal gap was never the real one.

NOT covered, deliberately: the branch where from-full masking reports a *comfortable* 3-7px with
zero contacts. That needs the eroded mask to sit more than 1.5px from the surviving one, and a
contiguous overlap cannot produce it — the covering shape's edge is always adjacent to where the
covered one reappears, so the naive gap comes out at 1.0px however the overlap is arranged. The
script keeps that branch (it is the right check on a real render, where a partly-covered edge
blends rather than stopping cleanly) and these tests pin the invariant that holds either way:
from-full masking over-reports the gap, never under-reports it.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from PIL import Image

SCRIPT = Path(__file__).with_name("measure_pixels.py")
WHITE, GRAY, BLUE = (255, 255, 255), (110, 117, 129), (40, 107, 187)

failures: list[str] = []


def check(name: str, got, want) -> None:
    if got == want:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}: got {got!r}, want {want!r}")
        failures.append(name)


def canvas(w: int = 60, h: int = 40) -> np.ndarray:
    return np.full((h, w, 3), WHITE, dtype=np.uint8)


def save(arr: np.ndarray, path: Path) -> Path:
    Image.fromarray(arr, mode="RGB").save(path)
    return path


def run(*args: str) -> tuple[int, dict]:
    import json

    proc = subprocess.run([sys.executable, str(SCRIPT), *args, "--json"], capture_output=True, text=True)
    payload = json.loads(proc.stdout) if proc.stdout.strip() else {}
    return proc.returncode, payload


def scene(tmp: Path, arrow_x: int, target_x: int, on_top: str = "arrow") -> dict[str, str]:
    """Four renders of a vertical arrow and a vertical target line, `on_top` painted last."""

    def paint(with_arrow: bool, with_target: bool) -> np.ndarray:
        img = canvas()
        layers = [("target", with_target, target_x, BLUE), ("arrow", with_arrow, arrow_x, GRAY)]
        if on_top == "target":
            layers.reverse()
        for _, present, x, color in layers:
            if present:
                img[10:30, x : x + 2] = color
        return img

    return {
        "full": str(save(paint(True, True), tmp / "full.png")),
        "no_arrow": str(save(paint(False, True), tmp / "no_arrow.png")),
        "no_target": str(save(paint(True, False), tmp / "no_target.png")),
        "no_both": str(save(paint(False, False), tmp / "no_both.png")),
    }


def test_clear_arrow(tmp: Path) -> None:
    print("a clear arrow, 4px of white between the two shapes")
    s = scene(tmp, arrow_x=20, target_x=26)  # arrow spans 20-21, target 26-27 -> gap 5px
    code, out = run(
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "0,0,60,40",
    )
    check("verdict", out.get("verdict"), "PASS")
    check("exit code", code, 0)
    check("min gap", out.get("min_gap"), 5.0)
    check("no contacts", out.get("touching_pairs"), 0)


def test_overlap_is_caught(tmp: Path) -> None:
    print("overlapping shapes, arrow painted ON TOP — the case from-full masking gets wrong")
    s = scene(tmp, arrow_x=25, target_x=26, on_top="arrow")
    code, out = run(
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "0,0,60,40",
        "--full",
        s["full"],
    )
    check("verdict", out.get("verdict"), "FAIL")
    check("exit code", code, 1)
    check("contacts reported", out.get("touching_pairs", 0) > 0, True)
    # The trap: the target is partly covered, so from-full masking sees a hole and over-reports.
    check("naive method disagreed", "note" in out, True)


def test_mismatched_full_render_is_unmeasurable(tmp: Path) -> None:
    print("a --full render at another size must be refused, not broadcast or silently believed")
    s = scene(tmp, arrow_x=20, target_x=26)
    args = (
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "0,0,60,40",
    )
    # Undersized: the crop comes out smaller and the diff used to raise a broadcasting ValueError.
    small = save(np.asarray(Image.open(s["full"]))[:20, :30].copy(), tmp / "full_small.png")
    code, _ = run(*args, "--full", str(small))
    check("undersized full exits 2", code, 2)

    # Oversized: a 2x export crops to the same box over a different part of the design, so the
    # diagnostic used to come out plausible and wrong rather than erroring at all.
    big = np.asarray(Image.open(s["full"]).resize((120, 80), Image.NEAREST))
    code, _ = run(*args, "--full", str(save(big.copy(), tmp / "full_2x.png")))
    check("oversized full exits 2", code, 2)


def test_negative_bounds_are_refused(tmp: Path) -> None:
    print("a box padded past the top-left must be refused, not indexed from the opposite edge")
    s = scene(tmp, arrow_x=20, target_x=26)
    code, _ = run(
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "-5,0,50,30",
    )
    check("arrow-gap exits 2", code, 2)
    # contrast used to raise from argmax() on the empty crop rather than report it.
    code, _ = run("contrast", "--png", s["full"], "--region", "-5,0,50,30")
    check("contrast exits 2", code, 2)
    # ink-box used to answer EMPTY with exit 0 — a "nothing in the margins" pass over zero pixels.
    code, _ = run("ink-box", "--png", s["full"], "--region", "0,-5,50,30")
    check("ink-box exits 2", code, 2)


def test_empty_mask_is_unmeasurable(tmp: Path) -> None:
    print("a crop containing neither shape must not read as a clean pass")
    s = scene(tmp, arrow_x=20, target_x=26)
    code, out = run(
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "0,32,20,40",
    )
    check("verdict", out.get("verdict"), "UNMEASURABLE")
    check("exit code", code, 2)
    check("names both shapes", "arrow and target" in out.get("reason", ""), True)


def test_bbox_guard(tmp: Path) -> None:
    print("a mask straying outside the declared bbox means a reflow, not a shape")
    s = scene(tmp, arrow_x=20, target_x=26)
    code, out = run(
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "0,0,60,40",
        "--arrow-bbox",
        "0,0,5,5",
    )
    check("verdict", out.get("verdict"), "UNMEASURABLE")
    check("exit code", code, 2)
    check("counts stray px", out.get("arrow_px_outside_bbox", 0) > 0, True)


def test_fractional_bbox_keeps_its_edge_pixels(tmp: Path) -> None:
    print("a fractional absoluteBoundingBox must not clip the anti-aliased columns it covers")
    # The arrow paints columns 20-21 and rows 10-29. Figma would report that box fractionally, and
    # rounding BOTH edges shrinks it to 20,10..21,29 — which excludes column 21 and row 29, so the
    # guard counts the shape's own ink as evidence of a reflow. Floor/ceil keeps 19,9..22,30.
    s = scene(tmp, arrow_x=20, target_x=26)
    code, out = run(
        "arrow-gap",
        "--no-arrow",
        s["no_arrow"],
        "--no-target",
        s["no_target"],
        "--no-both",
        s["no_both"],
        "--crop",
        "0,0,60,40",
        "--arrow-bbox",
        "19.6,9.6,21.4,29.4",
    )
    check("no stray px", out.get("arrow_px_outside_bbox"), 0)
    check("verdict", out.get("verdict"), "PASS")
    check("exit code", code, 0)


def test_unreadable_png_is_unmeasurable(tmp: Path) -> None:
    print("a missing or corrupt render is an input failure (2), never a failed check (1)")
    missing = tmp / "does_not_exist.png"
    code, _ = run("ink-box", "--png", str(missing))
    check("missing file exits 2", code, 2)
    # What a failed download actually leaves behind: an error page saved under a .png name.
    not_an_image = tmp / "error_page.png"
    not_an_image.write_text("<html><body>403 Forbidden</body></html>")
    code, _ = run("contrast", "--png", str(not_an_image))
    check("non-image exits 2", code, 2)
    # A truncated PNG parses its header and fails on the pixels, which is why `load()` is forced.
    good = save(canvas(10, 10), tmp / "whole.png")
    truncated = tmp / "truncated.png"
    truncated.write_bytes(good.read_bytes()[: -len(good.read_bytes()) // 3])
    code, _ = run("ink-box", "--png", str(truncated))
    check("truncated exits 2", code, 2)


def test_malformed_background_is_unmeasurable(tmp: Path) -> None:
    print("every malformed --background is bad input (2), not a failed check (1)")
    png = save(canvas(30, 20), tmp / "plain.png")
    # Three separate holes, one regex. `#fffffg` cleared the 6-digit gate and died inside
    # int(..., 16) — an uncaught ValueError exits 1, the code that means "the chart is wrong".
    # `###ffffff` cleared it too, because lstrip("#") strips every leading hash.
    for bad in ("#fffffg", "zzzzzz", "#12345g", "###ffffff", "#ffff", "#fffffff", "", "#"):
        for mode in ("ink-box", "contrast"):
            code, _ = run(mode, "--png", str(png), "--background", bad)
            check(f"{mode} {bad!r} exits 2", code, 2)
    # An EMPTY value is a typo, not an omission — truthiness used to swap the caller's declared
    # background for an inferred one, silently answering a question they did not ask.
    code, _ = run("ink-box", "--png", str(png), "--background", "")
    check("ink-box '' exits 2 rather than inferring", code, 2)
    # The forms that ARE valid keep working, with and without the hash.
    img = canvas(30, 20)
    img[5:10, 5:10] = (0, 0, 0)
    inked = save(img, tmp / "inked.png")
    for good in ("#ffffff", "ffffff", "#FFFFFF"):
        code, out = run("ink-box", "--png", str(inked), "--background", good, "--json")
        check(f"{good!r} measures", out.get("ink_box"), [5, 5, 10, 10])
        check(f"{good!r} exits 0", code, 0)


def test_numeric_flags_outside_their_range_are_refused(tmp: Path) -> None:
    print("a numeric flag outside its meaningful range is bad input (2), never a verdict")
    img = canvas(60, 40)
    img[10:30, 20:25] = (0, 0, 0)
    png = save(img, tmp / "square.png")
    code, out = run("ink-box", "--png", str(png), "--background", "#ffffff", "--json")
    check("true extent", out.get("ink_box"), [20, 10, 25, 30])
    check("true ink px", out.get("ink_px"), 100)
    check("exit code", code, 0)
    # Every pixel satisfies abs(delta) > -1, background included, so the box becomes the whole
    # region and the run still exits 0 — a confident, wrong answer about where the ink ends.
    for bad in ("-1", "-8", "256", "1000"):
        code, _ = run("ink-box", "--png", str(png), "--background", "#ffffff", "--tolerance", bad)
        check(f"--tolerance {bad} exits 2", code, 2)
    check("--tolerance 0 is still allowed", run("ink-box", "--png", str(png), "--background", "#ffffff")[0], 0)

    # #f0f0f0 on white is 1.14:1 — an unreadable hairline, and the case `contrast` exists to catch.
    hair = canvas(40, 20)
    hair[5:15, 20] = (240, 240, 240)
    hp = save(hair, tmp / "hairline.png")
    code, out = run("contrast", "--png", str(hp), "--background", "#ffffff", "--json")
    check("hairline is FAIL at the default bar", out.get("verdict"), "FAIL")
    check("and exits 1", code, 1)
    check("peak contrast", out.get("peak_contrast"), 1.14)
    # Under a bar of 1 every pixel clears it, background included, so the same unreadable hairline
    # comes back PASS with exit 0. Above 21 nothing can ever clear it.
    for bad in ("-1", "0", "0.5", "21.5", "100"):
        code, _ = run("contrast", "--png", str(hp), "--background", "#ffffff", "--bar", bad)
        check(f"--bar {bad} exits 2", code, 2)
    check("--bar 1 is the floor and is allowed", run("contrast", "--png", str(hp), "--background", "#ffffff", "--bar", "1")[0], 0)
    check("--bar 21 is the ceiling and is allowed", run("contrast", "--png", str(hp), "--background", "#ffffff", "--bar", "21")[0], 1)


def test_gray_target_would_defeat_color_classification(tmp: Path) -> None:
    print("a GRAY target — the case that killed colour-based classification — still masks cleanly")

    def paint(with_arrow: bool, with_target: bool) -> np.ndarray:
        img = canvas()
        if with_target:
            img[10:30, 26:28] = GRAY  # same colour as the arrow
        if with_arrow:
            img[10:30, 20:22] = GRAY
        img[35:37, 0:60] = GRAY  # gray gridline furniture inside the crop
        return img

    paths = {
        n: str(save(paint(a, t), tmp / f"g_{n}.png"))
        for n, (a, t) in {"no_arrow": (False, True), "no_target": (True, False), "no_both": (False, False)}.items()
    }
    code, out = run(
        "arrow-gap",
        "--no-arrow",
        paths["no_arrow"],
        "--no-target",
        paths["no_target"],
        "--no-both",
        paths["no_both"],
        "--crop",
        "0,0,60,40",
    )
    check("verdict", out.get("verdict"), "PASS")
    check("gridline not counted as arrow", out.get("arrow_px"), 40)  # 20 rows x 2 cols only
    check("target found despite same colour", out.get("target_px"), 40)


def test_subpixel_stroke_scale(tmp: Path) -> None:
    print("a faint hairline fails the bar at 1:1 and clears it when rendered larger")
    faint = canvas(20, 20)
    faint[5:15, 10] = (223, 223, 223)  # ~12% coverage, the spread a 0.3px stroke leaves
    code, out = run("contrast", "--png", str(save(faint, tmp / "faint.png")), "--background", "#ffffff", "--bar", "4.5")
    check("faint fails", out.get("verdict"), "FAIL")
    check("exit code", code, 1)
    check("ratio is low", out.get("peak_contrast", 99) < 1.5, True)
    check("advises a 4x render", "4x render" in out.get("reason", ""), True)

    solid = canvas(20, 20)
    solid[5:15, 10] = (70, 70, 70)  # what the same stroke resolves to at export scale
    code, out = run("contrast", "--png", str(save(solid, tmp / "solid.png")), "--background", "#ffffff", "--bar", "4.5")
    check("solid passes", out.get("verdict"), "PASS")
    check("exit code", code, 0)


def test_antialiased_gap_tracks_truth(tmp: Path) -> None:
    """Under realistic AA the masks must not inflate and the gap must track the real separation.

    Rendered by supersampling and downscaling with BOX (area average), which is what a renderer's
    anti-aliasing does. Do NOT use a ringing filter such as LANCZOS here: its overshoot spreads a
    `differs` mask two or three pixels past the shape, far enough that two separated shapes' masks
    overlap and every gap reads as 0. That is a property of the resampling, not of the check.
    """
    print("anti-aliased edges: measured gap tracks the true separation, masks stay put")
    S, W, H, target_x = 8, 80, 40, 40

    def render(arrow_right: int, with_arrow: bool, with_target: bool, name: str) -> str:
        a = np.full((H * S, W * S, 3), 255, dtype=np.uint8)
        if with_target:
            a[10 * S : 30 * S, target_x * S : (target_x + 2) * S] = BLUE
        if with_arrow:
            for row in range(10 * S, 30 * S):  # slants away from the target, so the top row is closest
                x1 = arrow_right * S - int((row - 10 * S) / (4 * S) * S)
                a[row, x1 - 2 * S : x1] = GRAY
        path = tmp / f"aa_{name}.png"
        Image.fromarray(a).resize((W, H), Image.BOX).save(path)
        return str(path)

    for blank_columns, want_gap in ((2, 3.0), (5, 6.0)):
        arrow_right = target_x - blank_columns
        paths = {
            key: render(arrow_right, arrow, tgt, f"{key}_{blank_columns}")
            for key, (arrow, tgt) in {
                "no_arrow": (False, True),
                "no_target": (True, False),
                "no_both": (False, False),
            }.items()
        }
        _, out = run(
            "arrow-gap",
            "--no-arrow",
            paths["no_arrow"],
            "--no-target",
            paths["no_target"],
            "--no-both",
            paths["no_both"],
            "--crop",
            "0,0,80,40",
        )
        # min_gap is the distance between pixel CENTRES, so it reads one more than the number of
        # blank columns between the two shapes' edges. Same convention as CHECKS.md's hypot().
        check(f"{blank_columns} blank columns -> gap", out.get("min_gap"), want_gap)
        check(f"{blank_columns} blank columns -> no contact", out.get("touching_pairs"), 0)
        # 20 rows x 2px arrow = 60 with the slant's stair-steps; the target bar is a clean 2x20.
        check(f"{blank_columns} blank columns -> arrow mask not inflated", out.get("arrow_px"), 60)
        check(f"{blank_columns} blank columns -> target mask not inflated", out.get("target_px"), 40)


def test_ink_box(tmp: Path) -> None:
    print("ink-box finds the true extent, and reports EMPTY rather than a bogus box")
    img = canvas(100, 50)
    img[10:20, 30:71] = BLUE  # x 30..70 inclusive -> width 41
    code, out = run("ink-box", "--png", str(save(img, tmp / "ink.png")), "--background", "#ffffff")
    check("verdict", out.get("verdict"), "INK")
    check("box", out.get("ink_box"), [30, 10, 71, 20])
    check("width", out.get("width"), 41)

    code, out = run("ink-box", "--png", str(save(canvas(10, 10), tmp / "blank.png")), "--background", "#ffffff")
    check("blank region", out.get("verdict"), "EMPTY")
    check("exit code", code, 0)


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        for test in (
            test_clear_arrow,
            test_overlap_is_caught,
            test_mismatched_full_render_is_unmeasurable,
            test_negative_bounds_are_refused,
            test_empty_mask_is_unmeasurable,
            test_bbox_guard,
            test_fractional_bbox_keeps_its_edge_pixels,
            test_unreadable_png_is_unmeasurable,
            test_malformed_background_is_unmeasurable,
            test_numeric_flags_outside_their_range_are_refused,
            test_gray_target_would_defeat_color_classification,
            test_subpixel_stroke_scale,
            test_antialiased_gap_tracks_truth,
            test_ink_box,
        ):
            test(tmp)
    print()
    if failures:
        print(f"{len(failures)} failing check(s): {', '.join(failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
