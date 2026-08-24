#!/usr/bin/env python3
"""Measure rendered PNGs so the model never has to look at one to get a number.

Three checks in CHECKS.md are arithmetic on pixels, and routing them through the model costs a
turn and an image each while being less accurate than the arithmetic. This does them locally:

  arrow-gap   the four-render arrow protocol — mask each shape by node identity, then report the
              minimum gap and the contact count between arrow ink and target ink.
  contrast    WCAG contrast of the darkest ink in a region against its background, for judging a
              hairline at the scale it will actually be exported at.
  ink-box     the bounding box of everything that paints in a region, for "nothing in the margins"
              and for settling sub-pixel disagreements about where a frame's ink really ends.

Usage (from the repo root, through the repo interpreter):

    .venv/bin/python .claude/skills/create-figma-chart/scripts/measure_pixels.py arrow-gap \\
        --no-arrow no_arrow.png --no-target no_target.png --no-both no_both.png \\
        --crop 120,240,300,360 [--full full.png] [--json]

    .venv/bin/python .claude/skills/create-figma-chart/scripts/measure_pixels.py contrast \\
        --png render.png --region 16,480,524,500 [--background '#ffffff'] [--bar 4.5]

    .venv/bin/python .claude/skills/create-figma-chart/scripts/measure_pixels.py ink-box \\
        --png render.png [--region 0,0,540,540] [--background '#ffffff']

Exit codes: 0 the check passed, 1 it failed, 2 the measurement is not trustworthy (an empty mask,
a guard violation, mismatched renders) — so a caller can tell "arrow is clear" from "I could not
tell", which a bare number cannot.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
from PIL import Image
from scipy import ndimage

# Pass is touching == 0 with a gap in this band (CHECKS.md). Below it the arrow crowds the line;
# above it stops reading as a pointer.
GAP_MIN, GAP_MAX = 3.0, 7.0
# radius 1.5 in CHECKS.md's `touching` is exactly the 3x3 neighbourhood: max hypot is 1.414.
CONTACT_KERNEL = np.ones((3, 3), dtype=np.int32)


def load(path: Path) -> np.ndarray:
    """RGB uint8, alpha composited over white so a transparent frame matches what Figma shows."""
    img = Image.open(path)
    if img.mode in ("RGBA", "LA", "P"):
        img = img.convert("RGBA")
        matte = Image.new("RGBA", img.size, (255, 255, 255, 255))
        img = Image.alpha_composite(matte, img)
    return np.asarray(img.convert("RGB"), dtype=np.uint8)


def parse_box(spec: str, what: str) -> tuple[int, int, int, int]:
    try:
        x0, y0, x1, y1 = (int(round(float(v))) for v in spec.split(","))
    except ValueError:
        raise SystemExit(f"{what} must be x0,y0,x1,y1 — got {spec!r}")
    if x1 <= x0 or y1 <= y0:
        raise SystemExit(f"{what} is empty: {spec!r}")
    return x0, y0, x1, y1


def as_hex(rgb: np.ndarray) -> str:
    r, g, b = (int(c) for c in rgb)
    return f"#{r:02x}{g:02x}{b:02x}"


def parse_hex(value: str) -> np.ndarray:
    h = value.lstrip("#")
    if len(h) != 6:
        raise SystemExit(f"--background must be a 6-digit hex color — got {value!r}")
    return np.array([int(h[i : i + 2], 16) for i in (0, 2, 4)], dtype=np.int16)


def relative_luminance(rgb: np.ndarray) -> np.ndarray:
    """WCAG 2.1 relative luminance. Accepts (...,3) uint8/int, returns (...) float."""
    srgb = np.asarray(rgb, dtype=np.float64) / 255.0
    linear = np.where(srgb <= 0.03928, srgb / 12.92, ((srgb + 0.055) / 1.055) ** 2.4)
    return linear @ np.array([0.2126, 0.7152, 0.0722])


def contrast_ratio(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    la, lb = relative_luminance(a), relative_luminance(b)
    hi, lo = np.maximum(la, lb), np.minimum(la, lb)
    return (hi + 0.05) / (lo + 0.05)


def crop_of(img: np.ndarray, box: tuple[int, int, int, int]) -> np.ndarray:
    x0, y0, x1, y1 = box
    return img[y0:y1, x0:x1]


def differs(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Boolean mask of pixels whose color changed at all — node identity, never color (CHECKS.md)."""
    return np.any(a != b, axis=-1)


def cmd_arrow_gap(args: argparse.Namespace) -> int:
    box = parse_box(args.crop, "--crop")
    renders = {
        name: load(Path(p))
        for name, p in (("no_arrow", args.no_arrow), ("no_target", args.no_target), ("no_both", args.no_both))
    }
    shapes = {name: img.shape for name, img in renders.items()}
    if len(set(shapes.values())) != 1:
        print(f"renders differ in size, so a pixel diff is meaningless: {shapes}", file=sys.stderr)
        return 2

    height, width = next(iter(shapes.values()))[:2]
    x0, y0, x1, y1 = box
    if x1 > width or y1 > height:
        print(f"--crop {args.crop} falls outside the {width}x{height} render", file=sys.stderr)
        return 2

    crops = {name: crop_of(img, box) for name, img in renders.items()}
    # Each shape is masked from the pass where the OTHER was already hidden. Diffing against the
    # full render instead leaves a hole exactly where the shapes overlap (CHECKS.md).
    arrow = differs(crops["no_target"], crops["no_both"])
    target = differs(crops["no_arrow"], crops["no_both"])

    result: dict[str, object] = {
        "crop": list(box),
        "arrow_px": int(arrow.sum()),
        "target_px": int(target.sum()),
    }

    if not arrow.any() or not target.any():
        missing = " and ".join(n for n, m in (("arrow", arrow), ("target", target)) if not m.any())
        result["verdict"] = "UNMEASURABLE"
        result["reason"] = f"no {missing} pixels in the crop — wrong bbox, wrong frame, or the hide never applied"
        emit(result, args.json)
        return 2

    # Exact Euclidean nearest-target distance for every cell, then the minimum over arrow ink.
    # CHECKS.md's all-pairs loop is the same number in O(n^2); this is O(n).
    dist = ndimage.distance_transform_edt(~target)
    min_gap = float(dist[arrow].min())
    # `touching` in CHECKS.md counts PAIRS within 1.5px, which is the 3x3 neighbourhood.
    neighbours = ndimage.convolve(target.astype(np.int32), CONTACT_KERNEL, mode="constant", cval=0)
    touching_pairs = int(neighbours[arrow].sum())
    arrow_px_touching = int((neighbours[arrow] > 0).sum())

    nearest = np.argwhere(arrow & (dist == dist[arrow].min()))
    result.update(
        min_gap=round(min_gap, 3),
        touching_pairs=touching_pairs,
        arrow_px_touching=arrow_px_touching,
        nearest_arrow_px=[int(x0 + nearest[0][1]), int(y0 + nearest[0][0])],
    )

    # Guard: a mask that strays outside the shape's own box is measuring a reflow, not the shape.
    for name, mask, spec in (("arrow", arrow, args.arrow_bbox), ("target", target, args.target_bbox)):
        if not spec:
            continue
        gx0, gy0, gx1, gy1 = parse_box(spec, f"--{name}-bbox")
        ys, xs = np.nonzero(mask)
        outside = int(((xs + x0 < gx0) | (xs + x0 >= gx1) | (ys + y0 < gy0) | (ys + y0 >= gy1)).sum())
        result[f"{name}_px_outside_bbox"] = outside
        if outside:
            result["verdict"] = "UNMEASURABLE"
            result["reason"] = (
                f"{outside} {name} pixels fall outside its declared absoluteBoundingBox — hiding "
                "the node reflowed something else, so the mask is measuring the reflow"
            )
            emit(result, args.json)
            return 2

    # Optional: catch the discredited from-full method silently disagreeing. Whichever node paints
    # on top leaves a hole in the OTHER's from-full mask, so the failure is under-reported contact.
    if args.full:
        full = crop_of(load(Path(args.full)), box)
        naive_arrow = differs(full, crops["no_arrow"])
        naive_target = differs(full, crops["no_target"])
        if naive_arrow.any() and naive_target.any():
            naive_gap = float(ndimage.distance_transform_edt(~naive_target)[naive_arrow].min())
            naive_contacts = int(
                ndimage.convolve(naive_target.astype(np.int32), CONTACT_KERNEL, mode="constant", cval=0)[
                    naive_arrow
                ].sum()
            )
            result["naive_from_full_min_gap"] = round(naive_gap, 3)
            result["naive_from_full_touching_pairs"] = naive_contacts
            if naive_contacts == 0 and touching_pairs > 0:
                result["note"] = (
                    f"masking from the full render would have MISSED this contact entirely — it "
                    f"reports {naive_gap:.1f}px clear with no contacts against {touching_pairs} "
                    "real pixel pair(s). This is the verdict the four-render protocol exists to "
                    "prevent (CHECKS.md)"
                )
            elif abs(naive_gap - min_gap) > 0.5:
                result["note"] = (
                    f"masking from the full render would have reported {naive_gap:.1f}px against "
                    f"{min_gap:.1f}px — the hole where the shapes overlap (CHECKS.md)"
                )

    passed = touching_pairs == 0 and GAP_MIN <= min_gap <= GAP_MAX
    result["verdict"] = "PASS" if passed else "FAIL"
    if not passed:
        if touching_pairs:
            result["reason"] = f"arrow and target ink touch at {touching_pairs} pixel pair(s)"
        else:
            side = "close" if min_gap < GAP_MIN else "far"
            result["reason"] = f"gap {min_gap:.1f}px is too {side} (want {GAP_MIN}-{GAP_MAX}px)"
    emit(result, args.json)
    return 0 if passed else 1


def cmd_contrast(args: argparse.Namespace) -> int:
    img = load(Path(args.png))
    height, width = img.shape[:2]
    box = parse_box(args.region, "--region") if args.region else (0, 0, width, height)
    if box[2] > width or box[3] > height:
        print(f"--region {args.region} falls outside the {width}x{height} render", file=sys.stderr)
        return 2
    region = crop_of(img, box).reshape(-1, 3)

    if args.background:
        background = parse_hex(args.background)
    else:
        # The modal color of a region that is mostly background.
        colors, counts = np.unique(region, axis=0, return_counts=True)
        background = colors[counts.argmax()].astype(np.int16)

    ratios = contrast_ratio(region, background)
    ink = ratios > 1.05  # anything visibly different from the background
    result: dict[str, object] = {
        "region": list(box),
        "background": as_hex(background),
        "px": int(region.shape[0]),
        "ink_px": int(ink.sum()),
    }
    if not ink.any():
        result["verdict"] = "UNMEASURABLE"
        result["reason"] = "no pixel in the region differs from the background"
        emit(result, args.json)
        return 2

    peak = float(ratios.max())
    result.update(
        peak_contrast=round(peak, 2),
        median_ink_contrast=round(float(np.median(ratios[ink])), 2),
        ink_px_clearing_bar=int((ratios >= args.bar).sum()),
        bar=args.bar,
    )
    passed = peak >= args.bar
    result["verdict"] = "PASS" if passed else "FAIL"
    if not passed:
        result["reason"] = (
            f"darkest ink reaches only {peak:.2f}:1 against {result['background']} — a sub-pixel "
            "stroke spreads its color over neighbouring pixels, so re-measure on a 4x render "
            "before calling it a defect (GOTCHAS.md)"
        )
    emit(result, args.json)
    return 0 if passed else 1


def cmd_ink_box(args: argparse.Namespace) -> int:
    img = load(Path(args.png))
    height, width = img.shape[:2]
    box = parse_box(args.region, "--region") if args.region else (0, 0, width, height)
    if box[2] > width or box[3] > height:
        print(f"--region {args.region} falls outside the {width}x{height} render", file=sys.stderr)
        return 2
    region = crop_of(img, box)

    if args.background:
        background = parse_hex(args.background)
    else:
        colors, counts = np.unique(region.reshape(-1, 3), axis=0, return_counts=True)
        background = colors[counts.argmax()].astype(np.int16)

    ink = np.any(np.abs(region.astype(np.int16) - background) > args.tolerance, axis=-1)
    result: dict[str, object] = {
        "region": list(box),
        "background": as_hex(background),
        "tolerance": args.tolerance,
        "ink_px": int(ink.sum()),
    }
    if not ink.any():
        result["verdict"] = "EMPTY"
        emit(result, args.json)
        return 0

    ys, xs = np.nonzero(ink)
    x0, y0 = box[0], box[1]
    result.update(
        ink_box=[int(x0 + xs.min()), int(y0 + ys.min()), int(x0 + xs.max() + 1), int(y0 + ys.max() + 1)],
        width=int(xs.max() - xs.min() + 1),
        height=int(ys.max() - ys.min() + 1),
        verdict="INK",
    )
    emit(result, args.json)
    return 0


def emit(result: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2))
        return
    order = ["verdict", "reason", "note"]
    for key in order:
        if key in result:
            print(f"{key}: {result[key]}")
    for key, value in result.items():
        if key not in order:
            print(f"{key}: {value}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)

    p = sub.add_parser("arrow-gap", help="four-render arrow protocol: gap and contact count")
    p.add_argument("--no-arrow", required=True, metavar="PNG", help="render with the arrow hidden")
    p.add_argument("--no-target", required=True, metavar="PNG", help="render with the target hidden")
    p.add_argument("--no-both", required=True, metavar="PNG", help="render with both hidden")
    p.add_argument("--full", metavar="PNG", help="optional; only to flag the from-full masking trap")
    p.add_argument("--crop", required=True, metavar="X0,Y0,X1,Y1", help="the arrow's padded bbox, in frame coords")
    p.add_argument("--arrow-bbox", metavar="X0,Y0,X1,Y1", help="guard: the arrow's absoluteBoundingBox")
    p.add_argument("--target-bbox", metavar="X0,Y0,X1,Y1", help="guard: the target's absoluteBoundingBox")
    p.set_defaults(func=cmd_arrow_gap)

    p = sub.add_parser("contrast", help="WCAG contrast of the darkest ink against its background")
    p.add_argument("--png", required=True)
    p.add_argument("--region", metavar="X0,Y0,X1,Y1")
    p.add_argument("--background", metavar="HEX", help="default: the region's modal color")
    p.add_argument("--bar", type=float, default=4.5, help="contrast to clear (default 4.5)")
    p.set_defaults(func=cmd_contrast)

    p = sub.add_parser("ink-box", help="bounding box of everything that paints in a region")
    p.add_argument("--png", required=True)
    p.add_argument("--region", metavar="X0,Y0,X1,Y1")
    p.add_argument("--background", metavar="HEX", help="default: the region's modal color")
    p.add_argument("--tolerance", type=int, default=8, help="per-channel delta that counts as ink")
    p.set_defaults(func=cmd_ink_box)

    for parser in sub.choices.values():
        parser.add_argument("--json", action="store_true", help="emit JSON instead of lines")

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
