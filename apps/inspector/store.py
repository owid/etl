"""Persist inspector findings to the ``inspections`` table.

The table is created dynamically on the current environment's DB (on a branch that is the
staging server, mirroring how anomalist works) — production is untouched. Findings are
deduplicated by fingerprint: re-storing a known finding updates ``lastSeenAt`` and preserves its
triage status, except that a dismissal expires when the inspected text changed.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from structlog import get_logger

from etl.config import OWID_ENV
from etl.db import read_sql
from etl.grapher.model import Inspection

log = get_logger()


def compute_fingerprint(finding: dict[str, Any]) -> str:
    """Stable id of a finding across runs: same origin, field, category, and (normalized)
    context means the same finding."""
    context = re.sub(r"\s+", " ", str(finding.get("context") or "")).strip().lower()[:120]
    key = "|".join(
        [
            str(finding.get("content_type") or ""),
            str(finding.get("slug") or ""),
            str(finding.get("origin_id") or ""),
            str(finding.get("field") or ""),
            str(finding.get("category") or ""),
            context,
        ]
    )
    return hashlib.sha256(key.encode()).hexdigest()[:40]


def load_findings_file(path: Path) -> list[dict[str, Any]]:
    findings = json.loads(path.read_text())
    if not isinstance(findings, list):
        raise ValueError(f"Findings file must contain a JSON list: {path}")
    return findings


def store_findings(
    findings: list[dict[str, Any]],
    inspected: list[tuple[str, str]] | None = None,
    engine: Engine | None = None,
) -> dict[str, int]:
    """Upsert findings and return summary counts.

    Args:
        findings: Finding dicts (see the inspector skill for the schema).
        inspected: Optional list of (content_type, slug) pairs fully inspected in this run. Open
            findings on those objects whose fingerprint was NOT seen again are marked ``fixed``.
        engine: Defaults to the current OWID_ENV (the branch's staging server).
    """
    engine = engine or OWID_ENV.get_engine()
    Inspection.create_table(engine, if_exists="skip")
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    counts = {"new": 0, "seen_again": 0, "reopened": 0, "still_dismissed": 0, "marked_fixed": 0}
    seen_fingerprints = set()

    with Session(engine) as session:
        for finding in findings:
            fingerprint = finding.get("fingerprint") or compute_fingerprint(finding)
            seen_fingerprints.add(fingerprint)
            existing = session.scalars(select(Inspection).where(Inspection.fingerprint == fingerprint)).first()
            content_hash = finding.get("content_hash")
            if existing:
                existing.lastSeenAt = now
                if existing.status == "dismissed":
                    if content_hash and existing.contentHash and content_hash != existing.contentHash:
                        # The text changed since the dismissal; it deserves a fresh look.
                        existing.status = "open"
                        counts["reopened"] += 1
                    else:
                        counts["still_dismissed"] += 1
                elif existing.status == "fixed":
                    # It came back: the fix never landed or regressed.
                    existing.status = "open"
                    counts["reopened"] += 1
                else:
                    counts["seen_again"] += 1
                if content_hash:
                    existing.contentHash = content_hash
            else:
                session.add(
                    Inspection(
                        fingerprint=fingerprint,
                        contentType=str(finding.get("content_type") or ""),
                        slug=str(finding.get("slug") or ""),
                        field=str(finding.get("field") or ""),
                        originId=str(finding.get("origin_id") or ""),
                        category=str(finding.get("category") or ""),
                        severity=str(finding.get("severity") or "medium"),
                        source=str(finding.get("source") or "agent"),
                        context=finding.get("context"),
                        explanation=finding.get("explanation"),
                        suggestedFix=finding.get("suggested_fix"),
                        affectedViews=finding.get("affected_views"),
                        url=finding.get("url"),
                        fixLocation=finding.get("fix_location"),
                        contentHash=content_hash,
                        lastSeenAt=now,
                    )
                )
                counts["new"] += 1

        # Open findings on fully-inspected objects that were not found again are fixed.
        if inspected:
            for content_type, slug in inspected:
                open_findings = session.scalars(
                    select(Inspection).where(
                        Inspection.contentType == content_type,
                        Inspection.slug == slug,
                        Inspection.status == "open",
                    )
                )
                for row in open_findings:
                    if row.fingerprint not in seen_fingerprints:
                        row.status = "fixed"
                        counts["marked_fixed"] += 1

        session.commit()

    log.info("inspector.store", **counts)
    return counts


def list_findings(
    status: str | None = "open",
    content_type: str | None = None,
    slug: str | None = None,
    engine: Engine | None = None,
) -> pd.DataFrame:
    engine = engine or OWID_ENV.get_engine()
    query = "SELECT * FROM inspections WHERE 1 = 1"
    params: dict[str, Any] = {}
    if status:
        query += " AND status = %(status)s"
        params["status"] = status
    if content_type:
        query += " AND contentType = %(content_type)s"
        params["content_type"] = content_type
    if slug:
        query += " AND slug = %(slug)s"
        params["slug"] = slug
    query += " ORDER BY severity = 'high' DESC, severity = 'medium' DESC, slug, field"
    try:
        return read_sql(query, engine, params=params or None)
    except Exception as e:
        if "1146" in str(e):
            # Table doesn't exist yet: no findings stored on this server.
            return pd.DataFrame()
        raise


def dismiss_findings(
    fingerprints: list[str],
    reason: str,
    dismissed_by: str | None = None,
    engine: Engine | None = None,
) -> int:
    """Mark findings as dismissed; they stay suppressed until the underlying text changes."""
    engine = engine or OWID_ENV.get_engine()
    dismissed = 0
    with Session(engine) as session:
        for fingerprint in fingerprints:
            # Allow fingerprint prefixes so the CLI is usable with the first few characters.
            rows = list(session.scalars(select(Inspection).where(Inspection.fingerprint.startswith(fingerprint))))
            if len(rows) > 1:
                raise ValueError(f"Fingerprint prefix '{fingerprint}' is ambiguous ({len(rows)} matches).")
            if not rows:
                log.warning("inspector.dismiss.not_found", fingerprint=fingerprint)
                continue
            rows[0].status = "dismissed"
            rows[0].dismissReason = reason
            rows[0].dismissedBy = dismissed_by
            dismissed += 1
        session.commit()
    return dismissed
