"""In-place tracked-changes text editor (custom bidirectional Streamlit component).

Renders the field's text as an editable area with a live Google-Docs-style
tracked-changes preview (deletions struck through, insertions tinted), plus an
optional comment box. Returns `{"action": "save"|"cancel", "text": ..., "comment":
..., "nonce": ...}` when the reviewer clicks a button; the `nonce` lets the caller
deduplicate across Streamlit reruns.
"""

from pathlib import Path
from typing import Any

import streamlit.components.v1 as components

_tracked_editor = components.declare_component(
    "metadata_review_tracked_editor",
    path=str(Path(__file__).parent / "frontend"),
)


def tracked_editor(
    original: str,
    initial: str,
    key: str,
    bullet_list: bool = False,
) -> dict[str, Any] | None:
    """Show the editor; `original` is the diff base (the field's current text),
    `initial` what the editable area starts with (current text or the existing
    proposal when refining)."""
    return _tracked_editor(
        original=original,
        initial=initial,
        bullet_list=bullet_list,
        key=key,
        default=None,
    )
