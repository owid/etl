"""Export metadata-review suggestions to the YAML handoff consumed by Claude.

The export runs inside the ETL repo, so it can do what the DB-only wizard can't:
trace every suggestion to a concrete edit location (file + YAML key path, or the
step .py when the value is generated programmatically) via `trace.py`.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.metadata_review.resolution import (
    check_staleness,
    resolve_dataset,
    resolve_mdim,
    shared_view_ids,
)
from apps.metadata_review.targets import MdimReview, ReviewableField
from apps.metadata_review.trace import EditCandidate, trace_indicator_field, trace_mdim_field
from etl.config import OWID_ENV, OWIDEnv
from etl.files import yaml_dump

log = structlog.get_logger()

INSTRUCTIONS = """\
Each suggestion below carries a resolved edit location. To implement one:

1. Apply `suggested_value` (or what the comments converge on) at `edit.file` /
   `edit.yaml_path`. Preserve YAML comments (ruamel-style editing).
2. If `edit.generated` is true, the value is built programmatically — edit the
   step .py; `edit.notes` lists the relevant call sites.
3. `edit.kind` explains how the value is built. For `jinja` templates, splice
   dimension words into the existing sentence instead of duplicating `<% if %>`
   branches. For `common-block` / `mdim-common-views` edits, check
   `edit.shared_with` / `affected_views` — the edit applies to all of them.
4. `edit.render_verified: true` means re-rendering the traced template reproduced
   the live value, so the location is confirmed. When false or absent, verify the
   location before editing.
5. Suggestions with `stale` flags were filed against an older version of the
   page — read the comments and re-check against the current text.
6. After implementing, rebuild the affected steps and mark the suggestion as
   implemented (wizard Metadata Review app, or `etl metadata-review resolve <id>
   --status implemented`).
"""


def build_export(
    session: Session,
    target: str,
    statuses: list[str] | None = None,
    owid_env: OWIDEnv | None = None,
) -> dict[str, Any]:
    """Build the export document for an MDim (catalogPath or slug) or a grapher dataset."""
    owid_env = owid_env or OWID_ENV
    target_type, target_path = _normalize_target(session, target)

    if target_type == "mdim":
        review = resolve_mdim(session, target_path, owid_env=owid_env)
        paths = [target_path] + review.indicator_paths
        target_info = {"type": "mdim", "catalog_path": target_path, "slug": review.slug, "title": review.title}
    else:
        review = resolve_dataset(session, target_path, owid_env=owid_env)
        paths = review.indicator_paths
        target_info = {"type": "dataset", "catalog_path": target_path, "name": review.name}

    suggestions = gm.MetadataReviewSuggestion.load_for_paths(session, paths, statuses=statuses)
    comments = gm.MetadataReviewComment.load_for_suggestions(session, [s.id for s in suggestions])
    comments_by_suggestion: dict[int, list[gm.MetadataReviewComment]] = {}
    for comment in comments:
        comments_by_suggestion.setdefault(comment.suggestionId, []).append(comment)
    users = _user_names(session, suggestions, comments)

    fields_by_key: dict[tuple, ReviewableField] = {}
    if isinstance(review, MdimReview):
        all_fields = review.all_fields
    else:
        all_fields = [f for ind in review.indicators for f in ind.fields]
    for field in all_fields:
        fields_by_key.setdefault(field.source_key(), field)

    trace_cache: dict[tuple, list[EditCandidate]] = {}
    entries = []
    for suggestion in suggestions:
        entries.append(
            _export_suggestion(
                session,
                suggestion,
                review if isinstance(review, MdimReview) else None,
                fields_by_key,
                comments_by_suggestion.get(suggestion.id, []),
                users,
                trace_cache,
            )
        )

    return {
        "metadata_review_export": {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "environment": owid_env.name,
            "target": target_info,
            "n_suggestions": len(entries),
            "instructions": INSTRUCTIONS,
        },
        "suggestions": entries,
    }


def export_to_yaml(document: dict[str, Any], output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_dump(document))  # type: ignore[arg-type]
    return output_path


def _normalize_target(session: Session, target: str) -> tuple[str, str]:
    """Resolve a user-supplied target to ('mdim'|'dataset', canonical catalogPath)."""
    # MDim catalogPath as stored ('ns/ver/short#short')?
    if "#" in target:
        if gm.MultiDimDataPage.load_mdim(session, catalogPath=target) is not None:
            return "mdim", target
        raise ValueError(f"No MDim with catalogPath '{target}' found in multi_dim_data_pages.")
    # MDim slug?
    mdim = session.scalars(select(gm.MultiDimDataPage).where(gm.MultiDimDataPage.slug == target)).one_or_none()
    if mdim is not None and mdim.catalogPath:
        return "mdim", mdim.catalogPath
    # Dataset catalogPath, with or without the 'grapher/' prefix.
    dataset_path = target.removeprefix("grapher/")
    dataset = session.scalars(select(gm.Dataset).where(gm.Dataset.catalogPath == dataset_path)).one_or_none()
    if dataset is not None:
        return "dataset", dataset_path
    raise ValueError(
        f"Target '{target}' is neither an MDim catalogPath/slug nor a grapher dataset catalogPath ('ns/version/dataset')."
    )


def _user_names(
    session: Session,
    suggestions: list[gm.MetadataReviewSuggestion],
    comments: list[gm.MetadataReviewComment],
) -> dict[int, str]:
    user_ids = {s.createdBy for s in suggestions} | {c.userId for c in comments}
    user_ids |= {s.resolvedBy for s in suggestions if s.resolvedBy is not None}
    if not user_ids:
        return {}
    rows = session.execute(select(gm.User.id, gm.User.fullName).where(gm.User.id.in_(user_ids))).all()
    return {user_id: full_name for user_id, full_name in rows}


def _export_suggestion(
    session: Session,
    suggestion: gm.MetadataReviewSuggestion,
    mdim_review: MdimReview | None,
    fields_by_key: dict[tuple, ReviewableField],
    comments: list[gm.MetadataReviewComment],
    users: dict[int, str],
    trace_cache: dict[tuple, list[EditCandidate]],
) -> dict[str, Any]:
    staleness = check_staleness(suggestion, fields_by_key)
    key = (suggestion.targetType, suggestion.targetPath, suggestion.viewId, suggestion.fieldPath)
    live_field = fields_by_key.get(key)
    trace_value = live_field.current_value if live_field is not None else suggestion.currentValue

    if key not in trace_cache:
        try:
            if suggestion.targetType == "indicator":
                trace = trace_indicator_field(session, suggestion.targetPath, suggestion.fieldPath, trace_value)
            else:
                trace = trace_mdim_field(suggestion.targetPath, suggestion.viewId, suggestion.fieldPath, trace_value)
            trace_cache[key] = trace.edits
        except Exception as e:
            log.warning("metadata_review.export.trace_failed", suggestion=suggestion.id, error=str(e))
            trace_cache[key] = []
    edits = trace_cache[key]

    entry: dict[str, Any] = {
        "id": suggestion.id,
        "status": suggestion.status,
        "field": suggestion.fieldPath,
        "target": {"type": suggestion.targetType, "path": suggestion.targetPath},
        "provenance": suggestion.provenance,
        "current_value": suggestion.currentValue,
        "suggested_value": suggestion.suggestedValue,
        "suggested_by": users.get(suggestion.createdBy, f"user {suggestion.createdBy}"),
        "created_at": suggestion.createdAt.isoformat(timespec="seconds"),
    }
    if suggestion.inheritedFromPath:
        entry["inherited_from"] = suggestion.inheritedFromPath
    if suggestion.viewId:
        entry["view_id"] = suggestion.viewId
    if suggestion.filedFromViewId:
        entry["filed_from_view"] = suggestion.filedFromViewId

    # Human-readable view selection + all surfaces the parameter renders on.
    if mdim_review is not None:
        views_by_id = {v.view_id: v for v in mdim_review.views}
        shown_view = views_by_id.get(suggestion.viewId or suggestion.filedFromViewId or "")
        if shown_view is not None:
            entry["view"] = mdim_review.human_dimensions(shown_view.dimensions)
        entry["affected_views"] = _affected_views(suggestion, mdim_review, live_field)

    if staleness.is_stale or staleness.page_changed:
        stale: dict[str, Any] = {
            "target_gone": staleness.target_gone,
            "field_changed": staleness.field_changed,
            "page_changed": staleness.page_changed,
        }
        if staleness.field_changed:
            stale["value_now"] = staleness.current_value
        entry["stale"] = stale

    if suggestion.status != "open":
        entry["resolved_by"] = users.get(suggestion.resolvedBy or -1)
        entry["resolved_at"] = suggestion.resolvedAt.isoformat(timespec="seconds") if suggestion.resolvedAt else None

    if edits:
        entry["edit"] = _edit_to_dict(edits[0])
        if len(edits) > 1:
            entry["other_edit_candidates"] = [_edit_to_dict(e) for e in edits[1:]]
    else:
        entry["edit"] = None

    entry["comments"] = [
        {
            "author": users.get(c.userId, f"user {c.userId}"),
            "at": c.createdAt.isoformat(timespec="seconds"),
            "text": c.text,
            **({"kind": c.kind} if c.kind != "comment" else {}),
        }
        for c in comments
    ]
    return entry


def _affected_views(
    suggestion: gm.MetadataReviewSuggestion,
    review: MdimReview,
    live_field: ReviewableField | None,
) -> list[str]:
    """All surfaces rendering the suggestion's text — one edit fixes them all.

    Matched by identical rendered value (not only the same source indicator):
    MDims fan one garden template out into a different expanded indicator per
    view, all rendering the same text.
    """
    if suggestion.targetType == "indicator":
        views = []
        for view in review.views:
            for field in view.fields:
                if field.source_key() == (
                    suggestion.targetType,
                    suggestion.targetPath,
                    suggestion.viewId,
                    suggestion.fieldPath,
                ) or (
                    live_field is not None
                    and field.field_path == live_field.field_path
                    and field.current_value is not None
                    and str(field.current_value).strip() == str(live_field.current_value or "").strip()
                ):
                    views.append(view.view_id)
                    break
        return views + [f"data page of {suggestion.targetPath}"]
    if suggestion.viewId and live_field is not None:
        return [suggestion.viewId] + shared_view_ids(review, live_field)
    return []


def _edit_to_dict(edit: EditCandidate) -> dict[str, Any]:
    out: dict[str, Any] = {"file": edit.file, "kind": edit.kind}
    if edit.yaml_path:
        out["yaml_path"] = edit.yaml_path
    out["exact_key_found"] = edit.exact_key_found
    if edit.generated:
        out["generated"] = True
    if edit.template is not None:
        out["template"] = edit.template
    if edit.render_context:
        out["render_context"] = edit.render_context
    if edit.supplied_by:
        out["supplied_by"] = edit.supplied_by
    if edit.render_verified is not None:
        out["render_verified"] = edit.render_verified
    if edit.shared_with:
        out["shared_with"] = edit.shared_with
    if edit.notes:
        out["notes"] = edit.notes
    return out
