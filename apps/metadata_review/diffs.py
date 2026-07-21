"""Compact diff helpers for suggestion rendering.

`description_key` is a markdown bullet list that can run to a dozen bullets;
repeating the whole list in every thread drowns the actual change. These helpers
reduce a proposal to only what changed, bullet by bullet.
"""

import difflib
import html
import re
from dataclasses import dataclass
from typing import Literal

DiffOp = Literal["keep", "add", "remove"]

# Google-Docs-style tracked changes: deletions struck through, insertions tinted.
_DEL_STYLE = "color:#b3261e;text-decoration:line-through;background:#fbe9e7;border-radius:2px;"
_INS_STYLE = "color:#0b6e4f;background:#e6f4ea;border-radius:2px;text-decoration:none;"


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


def _tokenize(text: str) -> list[str]:
    """Split into words + whitespace runs, so the diff rejoins losslessly."""
    return re.findall(r"\S+|\s+", text or "")


def word_diff_html(current: str | None, proposed: str | None) -> str:
    """The proposed text rendered AS the current text with tracked changes:
    removed words struck through, inserted words tinted — Google-Docs style."""
    a, b = _tokenize(current or ""), _tokenize(proposed or "")
    parts: list[str] = []
    matcher = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    for tag, a0, a1, b0, b1 in matcher.get_opcodes():
        if tag == "equal":
            parts.append(html.escape("".join(a[a0:a1])))
        else:
            removed = "".join(a[a0:a1]).strip()
            added = "".join(b[b0:b1]).strip()
            if removed:
                parts.append(f'<del style="{_DEL_STYLE}">{html.escape(removed)}</del> ')
            if added:
                parts.append(f'<ins style="{_INS_STYLE}">{html.escape(added)}</ins> ')
    return "".join(parts).strip()


def tracked_changes_html(current: str | None, proposed: str | None, is_bullet_list: bool = False) -> str:
    """Full tracked-changes rendering of a proposal, ready for st.html.

    Bullet-list fields diff bullet-by-bullet: changed bullets get inline word
    tracking, added/removed bullets are marked whole, unchanged runs collapse.
    """
    if not is_bullet_list:
        body = word_diff_html(current, proposed)
        return f'<div style="line-height:1.55;font-size:0.95rem;">{body}</div>'

    lines: list[str] = []
    ops = bullet_diff(current, proposed)
    i, keep_run = 0, 0

    def flush_keeps() -> None:
        nonlocal keep_run
        if keep_run:
            noun = "bullet" if keep_run == 1 else "bullets"
            lines.append(f'<li style="color:#888;list-style:none;">… {keep_run} unchanged {noun}</li>')
            keep_run = 0

    while i < len(ops):
        op = ops[i]
        if op.op == "keep":
            keep_run += 1
            i += 1
        elif op.op == "remove" and i + 1 < len(ops) and ops[i + 1].op == "add":
            # A changed bullet: render as one bullet with inline word tracking.
            flush_keeps()
            lines.append(f"<li>{word_diff_html(op.text, ops[i + 1].text)}</li>")
            i += 2
        elif op.op == "remove":
            flush_keeps()
            lines.append(f'<li><del style="{_DEL_STYLE}">{html.escape(op.text)}</del></li>')
            i += 1
        else:
            flush_keeps()
            lines.append(f'<li><ins style="{_INS_STYLE}">{html.escape(op.text)}</ins></li>')
            i += 1
    flush_keeps()
    return '<ul style="line-height:1.55;font-size:0.95rem;margin:0;padding-left:1.2rem;">' + "".join(lines) + "</ul>"


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
