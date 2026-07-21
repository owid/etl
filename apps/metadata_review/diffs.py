"""Compact diff helpers for suggestion rendering.

`description_key` is a markdown bullet list that can run to a dozen bullets;
repeating the whole list in every thread drowns the actual change. These helpers
reduce a proposal to only what changed, bullet by bullet.
"""

import difflib
from dataclasses import dataclass
from typing import Literal

DiffOp = Literal["keep", "add", "remove"]


@dataclass
class BulletDiff:
    op: DiffOp
    text: str


def split_bullets(value: str | None) -> list[str]:
    """Split a markdown bullet-list string into bullets.

    A single description_key item is stored as prose (no leading "- "); several
    items as "- " lines. Continuation lines are folded into the preceding bullet.
    """
    if not value or not value.strip():
        return []
    lines = value.splitlines()
    if not any(line.lstrip().startswith("- ") for line in lines):
        return [value.strip()]
    bullets: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
        elif bullets:
            bullets[-1] += " " + stripped
        else:
            bullets.append(stripped)
    return bullets


def bullet_diff(current: str | None, proposed: str | None) -> list[BulletDiff]:
    """Bullet-level diff between the current and proposed markdown lists."""
    current_bullets = split_bullets(current)
    proposed_bullets = split_bullets(proposed)
    ops: list[BulletDiff] = []
    matcher = difflib.SequenceMatcher(a=current_bullets, b=proposed_bullets, autojunk=False)
    for tag, a0, a1, b0, b1 in matcher.get_opcodes():
        if tag == "equal":
            ops.extend(BulletDiff("keep", b) for b in current_bullets[a0:a1])
        else:
            ops.extend(BulletDiff("remove", b) for b in current_bullets[a0:a1])
            ops.extend(BulletDiff("add", b) for b in proposed_bullets[b0:b1])
    return ops


def diff_summary(ops: list[BulletDiff]) -> str:
    """One-line summary, e.g. '2 added, 1 removed; 4 unchanged'."""
    n_add = sum(1 for op in ops if op.op == "add")
    n_remove = sum(1 for op in ops if op.op == "remove")
    n_keep = sum(1 for op in ops if op.op == "keep")
    changed = ", ".join(
        part for part in [f"{n_add} added" if n_add else "", f"{n_remove} removed" if n_remove else ""] if part
    )
    return f"{changed or 'no bullet changes'}; {n_keep} unchanged"


def diff_markdown_lines(ops: list[BulletDiff], collapse_keeps: bool = True) -> list[str]:
    """Markdown lines showing only the changed bullets; unchanged runs are summarized."""
    lines: list[str] = []
    keep_run = 0

    def flush_keeps() -> None:
        nonlocal keep_run
        if keep_run:
            noun = "bullet" if keep_run == 1 else "bullets"
            lines.append(f"- _… {keep_run} unchanged {noun}_")
            keep_run = 0

    for op in ops:
        if op.op == "keep":
            if collapse_keeps:
                keep_run += 1
            else:
                lines.append(f"- {op.text}")
        elif op.op == "remove":
            flush_keeps()
            lines.append(f"- :red[−] ~~{op.text}~~")
        else:
            flush_keeps()
            lines.append(f"- :green[+] {op.text}")
    flush_keeps()
    return lines
