"""Deterministic lint pass over content bundles: codespell plus a few precise rule-based checks.

No LLM involved; this runs everywhere at zero cost. Findings use the same dict shape as
agent-produced findings (see the inspector skill), with ``source: "lint"``.
"""

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from structlog import get_logger

from apps.inspector.schema import ContentBundle, TextField
from etl.paths import BASE_DIR

log = get_logger()

CONTEXT_CHARS = 80

# Space(s) between two non-space characters beyond a single one (excludes markdown's
# end-of-line double space).
DOUBLE_SPACE_RE = re.compile(r"(?<=\S)  +(?=\S)")

# Leftover templating that should have been rendered before publication.
TEMPLATE_REMNANT_RE = re.compile(r"\{\{|\}\}|\{%|%\}|<%|<<[a-zA-Z_]+>>")

# A duplicated small word ("the the", "of of"). Restricted to common function words to avoid
# false positives like "had had" in legitimate prose.
REPEATED_WORD_RE = re.compile(
    r"\b(the|a|an|is|are|was|were|of|to|in|and|or|for|with|on|that|this|it|its)\s+\1\b",
    re.IGNORECASE,
)


def _context(text: str, start: int, end: int) -> str:
    lo = max(0, start - CONTEXT_CHARS)
    hi = min(len(text), end + CONTEXT_CHARS)
    snippet = text[lo:hi].replace("\n", " ")
    prefix = "…" if lo > 0 else ""
    suffix = "…" if hi < len(text) else ""
    return f"{prefix}{snippet}{suffix}"


def _base_finding(bundle: ContentBundle, text_field: TextField) -> dict[str, Any]:
    return {
        "content_type": bundle.kind,
        "slug": bundle.slug,
        "url": bundle.url,
        "field": text_field.name,
        "origin_id": text_field.origin_id,
        "fix_location": text_field.fix_location,
        "content_hash": bundle.content_hash,
        "source": "lint",
    }


def _lint_rules(bundle: ContentBundle) -> list[dict[str, Any]]:
    findings = []
    for text_field in bundle.all_fields():
        text = text_field.text
        is_markdown = text_field.origin == "markdown"

        for match in DOUBLE_SPACE_RE.finditer(text):
            findings.append(
                {
                    **_base_finding(bundle, text_field),
                    "category": "formatting-artifact",
                    "severity": "low",
                    "context": _context(text, match.start(), match.end()),
                    "explanation": "Multiple consecutive spaces.",
                    "suggested_fix": "Collapse to a single space.",
                }
            )

        # Post markdown legitimately contains curly-brace component syntax; skip the template
        # rule there.
        if not is_markdown:
            for match in TEMPLATE_REMNANT_RE.finditer(text):
                findings.append(
                    {
                        **_base_finding(bundle, text_field),
                        "category": "formatting-artifact",
                        "severity": "high",
                        "context": _context(text, match.start(), match.end()),
                        "explanation": f"Unrendered template placeholder '{match.group()}' in published text.",
                        "suggested_fix": "Render or remove the placeholder.",
                    }
                )

        for match in REPEATED_WORD_RE.finditer(text):
            findings.append(
                {
                    **_base_finding(bundle, text_field),
                    "category": "typo",
                    "severity": "medium",
                    "context": _context(text, match.start(), match.end()),
                    "explanation": f"Duplicated word: '{match.group()}'.",
                    "suggested_fix": f"Remove the repeated '{match.group(1)}'.",
                }
            )
    return findings


def _lint_codespell(bundles: list[ContentBundle]) -> list[dict[str, Any]]:
    """Run codespell once over every text field of every bundle."""
    # Flat index of fields so codespell file names map back to (bundle, field).
    indexed: list[tuple[ContentBundle, TextField]] = []
    for bundle in bundles:
        for text_field in bundle.all_fields():
            if text_field.text.strip():
                indexed.append((bundle, text_field))
    if not indexed:
        return []

    findings = []
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        for i, (_, text_field) in enumerate(indexed):
            (temp_path / f"{i}.txt").write_text(text_field.text)

        cmd = [str(BASE_DIR / ".venv" / "bin" / "codespell"), temp_dir]
        ignore_file = BASE_DIR / ".codespell-ignore.txt"
        if ignore_file.exists():
            cmd.extend(["--ignore-words", str(ignore_file)])
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Output lines look like: /tmp/dir/12.txt:3: typo ==> correction
        for line in result.stdout.strip().split("\n"):
            if "==>" not in line:
                continue
            left, correction = line.split("==>", 1)
            correction = correction.strip()
            file_part, _, typo = left.rsplit(":", 2)
            typo = typo.strip()
            try:
                index = int(Path(file_part).stem)
                bundle, text_field = indexed[index]
            except (ValueError, IndexError):
                log.warning("inspector.lint.unparsable_codespell_line", line=line)
                continue
            position = text_field.text.lower().find(typo.lower())
            if position < 0:
                continue
            findings.append(
                {
                    **_base_finding(bundle, text_field),
                    "category": "typo",
                    "severity": "medium",
                    "context": _context(text_field.text, position, position + len(typo)),
                    "explanation": f"Possible misspelling: '{typo}' should be '{correction}'.",
                    "suggested_fix": f"Replace '{typo}' with '{correction}'.",
                }
            )
    return findings


def lint(bundles: list[ContentBundle]) -> list[dict[str, Any]]:
    """Run all deterministic checks over the given bundles."""
    findings = _lint_codespell(bundles)
    for bundle in bundles:
        findings.extend(_lint_rules(bundle))
    log.info("inspector.lint", bundles=len(bundles), findings=len(findings))
    return findings
