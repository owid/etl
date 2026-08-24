#!/usr/bin/env python3
"""Tests for measure_pixels.py, on synthetic renders built to reproduce CHECKS.md's failure modes.

Run:  .venv/bin/python .claude/skills/create-figma-chart/scripts/test_measure_pixels.py

The cases that matter are not "does it compute a distance" but the four traps the prose documents:
masking from the full render hides the overlap being tested for; classifying pixels by color
collects the wrong shape; an empty mask must be UNMEASURABLE rather than a clean pass; and a
sub-pixel stroke reads as a defect at 1:1 and clears the bar at 4x.

NOT covered, deliberately: the branch where from-full masking reports a *comfortable* 3-7px with
zero contacts. That needs the eroded mask to sit more than 1.5px from the surviving one, and with
hard-edged shapes it cannot — a contiguous overlap always leaves the covering shape's edge adjacent
to where the covered one reappears, so the naive gap comes out at 1.0px however the overlap is
arranged. The real case that motivated the rule had anti-aliased edges. So the script keeps that
branch (it is the right check on real renders) and these tests only pin the invariant that holds
either way: from-full masking over-reports the gap, never under-reports it.
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
            test_empty_mask_is_unmeasurable,
            test_bbox_guard,
            test_gray_target_would_defeat_color_classification,
            test_subpixel_stroke_scale,
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
