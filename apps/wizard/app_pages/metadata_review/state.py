"""DB read/write wrappers for the Metadata Review wizard app.

Reads that are expensive and stable within a session (page resolution, listings)
are cached; suggestion/comment reads stay uncached so writes show up on the next
rerun without cache gymnastics.
"""

import streamlit as st
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.metadata_review.resolution import (
    list_datasets,
    list_mdims,
    resolve_dataset,
    resolve_mdim,
)
from apps.metadata_review.targets import DatasetReview, MdimReview, ReviewableField
from apps.wizard.utils.cached import get_grapher_user
from etl.db import get_engine


@st.cache_data(show_spinner=False, ttl="5m")
def review_tables_exist() -> bool:
    """The metadata_review_* tables ship via an owid-grapher migration; until that
    reaches production, staging DBs (prod copies) don't have them."""
    from sqlalchemy import inspect as sa_inspect

    return sa_inspect(get_engine()).has_table("metadata_review_suggestions")


@st.cache_data(show_spinner="Loading MDims...", ttl="5m")
def cached_list_mdims() -> list[dict]:
    with Session(get_engine()) as session:
        return list_mdims(session)


@st.cache_data(show_spinner="Loading datasets...", ttl="5m")
def cached_list_datasets() -> list[dict]:
    with Session(get_engine()) as session:
        return list_datasets(session)


@st.cache_data(show_spinner="Resolving fields...", ttl="2m")
def cached_resolve_mdim(catalog_path: str) -> MdimReview:
    with Session(get_engine()) as session:
        return resolve_mdim(session, catalog_path)


@st.cache_data(show_spinner="Resolving indicators...", ttl="2m")
def cached_resolve_dataset(catalog_path: str) -> DatasetReview:
    with Session(get_engine()) as session:
        return resolve_dataset(session, catalog_path)


def load_suggestions(target_paths: list[str]) -> tuple[list, dict[int, list], dict[int, str]]:
    """Suggestions for the given paths + their comments + user id -> name map. Uncached."""
    with Session(get_engine()) as session:
        suggestions = gm.MetadataReviewSuggestion.load_for_paths(session, target_paths)
        comments = gm.MetadataReviewComment.load_for_suggestions(session, [s.id for s in suggestions])
        comments_by_suggestion: dict[int, list] = {}
        for comment in comments:
            comments_by_suggestion.setdefault(comment.suggestionId, []).append(comment)
        user_ids = {s.createdBy for s in suggestions} | {c.userId for c in comments}
        users = {}
        if user_ids:
            from sqlalchemy import select

            rows = session.execute(select(gm.User.id, gm.User.fullName).where(gm.User.id.in_(user_ids))).all()
            users = dict(rows)
        # Detach ORM objects so they can be used after the session closes.
        session.expunge_all()
    return suggestions, comments_by_suggestion, users


def load_open_summary() -> list[dict]:
    """Open proposals grouped by the page they were filed from (for the landing overview).

    Returns [{page, kind ('mdim'|'indicator'), n_open, last_activity}] sorted by recency.
    Uncached so a freshly filed proposal shows up when the reviewer returns to the landing.
    """
    with Session(get_engine()) as session:
        from sqlalchemy import select

        rows = session.execute(
            select(
                gm.MetadataReviewSuggestion.filedFromPath,
                gm.MetadataReviewSuggestion.targetPath,
                gm.MetadataReviewSuggestion.targetType,
                gm.MetadataReviewSuggestion.updatedAt,
            ).where(gm.MetadataReviewSuggestion.status == "open")
        ).all()
    groups: dict[str, dict] = {}
    for filed_from, target_path, target_type, updated_at in rows:
        page = filed_from or target_path
        # A page path with '#' but no channel prefix is an MDim catalogPath.
        kind = "mdim" if ("#" in page and not page.startswith("grapher/")) else "indicator"
        group = groups.setdefault(page, {"page": page, "kind": kind, "n_open": 0, "last_activity": updated_at})
        group["n_open"] += 1
        group["last_activity"] = max(group["last_activity"], updated_at)
    return sorted(groups.values(), key=lambda g: g["last_activity"], reverse=True)


def open_counts_by_page() -> dict[str, int]:
    """Open-proposal counts keyed by the page the suggestion was filed from."""
    return {g["page"]: g["n_open"] for g in load_open_summary()}


def current_user() -> gm.User | None:
    """The grapher user (Tailscale IP on staging, GRAPHER_USER_ID locally); None when unknown."""
    try:
        return get_grapher_user()
    except Exception:
        return None


def create_suggestion(
    field: ReviewableField,
    user_id: int,
    suggested_value: str | None,
    comment_text: str | None,
) -> None:
    """File onto the field's consolidated thread (one open proposal per field)."""
    target_type, target_path, view_id, field_path = field.source_key()
    with Session(get_engine()) as session:
        gm.MetadataReviewSuggestion.file_or_update(
            session,
            target_type=target_type,
            target_path=target_path,
            view_id=view_id,
            field_path=field_path,
            user_id=user_id,
            provenance=field.provenance,
            current_value=str(field.current_value) if field.current_value is not None else None,
            suggested_value=suggested_value or None,
            comment_text=comment_text,
            inherited_from_path=field.inherited_from,
            filed_from_path=field.target_path,
            filed_from_view_id=field.view_id,
            page_checksum=field.page_checksum,
        )


def add_comment(suggestion_id: int, user_id: int, text: str) -> None:
    with Session(get_engine()) as session:
        suggestion = session.get(gm.MetadataReviewSuggestion, suggestion_id)
        assert suggestion is not None
        suggestion.add_comment(session, user_id=user_id, text=text)


def set_status(suggestion_id: int, user_id: int, status: str) -> None:
    with Session(get_engine()) as session:
        suggestion = session.get(gm.MetadataReviewSuggestion, suggestion_id)
        assert suggestion is not None
        suggestion.set_status(session, status, user_id=user_id)  # type: ignore[arg-type]
