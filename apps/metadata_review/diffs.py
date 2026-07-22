"""Diff helpers for suggestion rendering.

Proposals render as tracked changes over the FULL text — the reader always sees
the entire field as it would read, with deletions struck through and insertions
tinted. `description_key` (a markdown bullet list) diffs bullet-by-bullet so a
rewritten bullet tracks inline instead of appearing as a remove+add pair.
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
            # Leading space so a change never fuses with the preceding word when the
            # diff consumed the whitespace token (HTML collapses doubled spaces).
            if removed:
                parts.append(f' <del style="{_DEL_STYLE}">{html.escape(removed)}</del> ')
            if added:
                parts.append(f' <ins style="{_INS_STYLE}">{html.escape(added)}</ins> ')
    return "".join(parts).strip()


def tracked_changes_html(current: str | None, proposed: str | None, is_bullet_list: bool = False) -> str:
    """Full tracked-changes rendering of a proposal, ready for st.html.

    Bullet-list fields diff bullet-by-bullet: changed bullets get inline word
    tracking, added/removed bullets are marked whole, and unchanged bullets are
    shown in full — the reader always sees the entire text as it would read.
    """
    if not is_bullet_list:
        body = word_diff_html(current, proposed)
        return f'<div style="line-height:1.55;font-size:0.95rem;">{body}</div>'

    lines: list[str] = []
    for kind, removed, added in paired_ops(bullet_diff(current, proposed)):
        if kind == "keep":
            lines.append(f"<li>{html.escape(removed or '')}</li>")
        elif kind == "pair":
            # A changed bullet: render as one bullet with inline word tracking.
            lines.append(f"<li>{word_diff_html(removed, added)}</li>")
        elif kind == "remove":
            lines.append(f'<li><del style="{_DEL_STYLE}">{html.escape(removed or "")}</del></li>')
        else:
            lines.append(f'<li><ins style="{_INS_STYLE}">{html.escape(added or "")}</ins></li>')
    return '<ul style="line-height:1.55;font-size:0.95rem;margin:0;padding-left:1.2rem;">' + "".join(lines) + "</ul>"


def paired_ops(ops: list[BulletDiff]) -> list[tuple[str, str | None, str | None]]:
    """Regroup a bullet-diff into edit units, pairing removes with adds positionally.

    SequenceMatcher emits a replace block as ALL removes followed by ALL adds;
    pairing them index-wise turns that into per-bullet rewrites. Yields tuples
    ("keep", text, None) | ("pair", removed, added) | ("remove", removed, None)
    | ("add", None, added).
    """
    units: list[tuple[str, str | None, str | None]] = []
    i = 0
    while i < len(ops):
        if ops[i].op == "keep":
            units.append(("keep", ops[i].text, None))
            i += 1
            continue
        removes: list[str] = []
        adds: list[str] = []
        while i < len(ops) and ops[i].op == "remove":
            removes.append(ops[i].text)
            i += 1
        while i < len(ops) and ops[i].op == "add":
            adds.append(ops[i].text)
            i += 1
        for j in range(max(len(removes), len(adds))):
            removed = removes[j] if j < len(removes) else None
            added = adds[j] if j < len(adds) else None
            if removed is not None and added is not None:
                units.append(("pair", removed, added))
            elif removed is not None:
                units.append(("remove", removed, None))
            else:
                units.append(("add", None, added))
    return units


def apply_bullet_edits(
    proposal_current: str | None,
    proposal_suggested: str | None,
    target_current: str | None,
) -> tuple[str, int, int] | None:
    """Re-apply a bullet-list proposal to ANOTHER field's bullet list.

    Different pages (e.g. two MDims built from the same garden metadata) often
    share individual bullets via YAML anchors while their full lists differ. Each
    edit unit (a bullet rewrite/removal, or an addition anchored after a bullet)
    carries over when its bullet exists verbatim in the target list; units
    touching page-specific bullets are skipped. Returns
    (transferred text, edits applied, edits in the proposal), or None when
    nothing applies.
    """
    ops = bullet_diff(proposal_current, proposal_suggested)
    if all(op.op == "keep" for op in ops):
        return None
    target = split_bullets(target_current)

    def find(bullet: str) -> int:
        try:
            return target.index(bullet)
        except ValueError:
            return -1

    units = paired_ops(ops)
    # Pure additions may carry page-specific content (e.g. a bullet about one
    # dimension's data availability). They only ride along when EVERY rewrite in
    # the proposal applied here — a partial match means this page's list is a
    # different variant, and injected additions would mix contexts.
    rewrites = [(kind, removed) for kind, removed, _ in units if kind in ("pair", "remove")]
    all_rewrites_apply = all(find(removed or "") >= 0 for _, removed in rewrites)

    anchor = -1  # last position in `target` we matched or touched.
    anchored = False
    applied, total = 0, 0
    for kind, removed, added in units:
        if kind == "keep":
            pos = find(removed or "")
            if pos >= 0:
                anchor = pos
                anchored = True
            continue
        total += 1
        if kind in ("pair", "remove"):
            pos = find(removed or "")
            if pos < 0:
                continue  # page-specific bullet — this edit doesn't apply here.
            if kind == "pair" and added is not None:
                target[pos] = added
                anchor = pos
            else:
                del target[pos]
                anchor = pos - 1
            anchored = True
            applied += 1
        elif added is not None:  # pure addition
            if not anchored or not all_rewrites_apply:
                continue
            target.insert(anchor + 1, added)
            anchor += 1
            applied += 1

    if applied == 0:
        return None
    text = target[0] if len(target) == 1 else "\n".join(f"- {b}" for b in target)
    return text, applied, total
