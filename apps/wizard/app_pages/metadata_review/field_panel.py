"""The field row: provenance badge, current text, threads, and suggestion controls."""

import hashlib

import streamlit as st

import etl.grapher.model as gm
from apps.metadata_review.resolution import Staleness, check_staleness
from apps.metadata_review.targets import MdimReview, ReviewableField
from apps.wizard.app_pages.metadata_review import state

PROVENANCE_BADGES = {
    "override": ("green", "set in the MDim config"),
    "inherited": ("blue", "inherited from the indicator (garden metadata)"),
    "missing": ("red", "not set anywhere"),
}
STATUS_ICONS = {"open": "💬", "implemented": "✅", "rejected": "🚫"}


def _key(field: ReviewableField, suffix: str) -> str:
    digest = hashlib.md5("|".join(str(p) for p in field.source_key()).encode()).hexdigest()[:12]
    return f"mrf_{digest}_{suffix}"


def render_field(
    field: ReviewableField,
    suggestions: list[gm.MetadataReviewSuggestion],
    comments_by_suggestion: dict[int, list],
    users: dict[int, str],
    user: gm.User | None,
    fields_by_key: dict[tuple, ReviewableField],
    shared_views: list[str] | None = None,
    mdim_review: MdimReview | None = None,
) -> None:
    """Render one reviewable field with its threads and controls."""
    color, explanation = PROVENANCE_BADGES[field.provenance]
    header = st.container(border=True)
    with header:
        title_col, badge_col = st.columns([4, 2], vertical_alignment="center")
        with title_col:
            st.markdown(f"**{field.label}**")
        with badge_col:
            badges = [f":{color}-badge[{field.provenance}]"]
            if suggestions:
                n_open = sum(1 for s in suggestions if s.status == "open")
                if n_open:
                    badges.append(f":orange-badge[{n_open} open]")
            if shared_views:
                badges.append(f":gray-badge[appears in {len(shared_views) + 1} views]")
            st.markdown(" ".join(badges))

        if field.current_value is None:
            st.caption("_(not set — the chart renders without it)_")
        elif str(field.current_value) == "":
            st.caption("_(explicitly blank — the view suppresses the inherited text)_")
        else:
            st.markdown(f"> {field.current_value}")
        st.caption(f"{explanation.capitalize()}. {field.edit_hint}")

        if field.provenance == "override" and field.inherited_value is not None:
            with st.expander("Inherited value this override replaces", expanded=False):
                st.markdown(f"> {field.inherited_value}")
                st.caption(f"From `{field.inherited_from}`")

        if shared_views and mdim_review is not None:
            with st.expander(f"Also shown in {len(shared_views)} other view(s)", expanded=False):
                views_by_id = {v.view_id: v for v in mdim_review.views}
                for view_id in shared_views:
                    view = views_by_id.get(view_id)
                    dims = mdim_review.human_dimensions(view.dimensions) if view else {}
                    st.markdown("- " + (" · ".join(f"**{k}:** {v}" for k, v in dims.items()) or view_id))

        for suggestion in suggestions:
            _render_thread(suggestion, comments_by_suggestion.get(suggestion.id, []), users, user, fields_by_key)

        _render_new_suggestion_control(field, user)


def _render_thread(
    suggestion: gm.MetadataReviewSuggestion,
    comments: list,
    users: dict[int, str],
    user: gm.User | None,
    fields_by_key: dict[tuple, ReviewableField],
) -> None:
    staleness: Staleness = check_staleness(suggestion, fields_by_key)
    icon = STATUS_ICONS.get(suggestion.status, "💬")
    author = users.get(suggestion.createdBy, f"user {suggestion.createdBy}")
    label = f"{icon} #{suggestion.id} by {author} — {suggestion.status}"
    if staleness.is_stale:
        label += " ⚠️ stale"

    with st.expander(label, expanded=suggestion.status == "open"):
        if staleness.target_gone:
            st.warning("The view/indicator this suggestion was filed on no longer exists on this page.")
        elif staleness.field_changed:
            st.warning("The field changed since this suggestion was filed.")
            col_then, col_now = st.columns(2)
            with col_then:
                st.caption("Text when filed")
                st.markdown(f"> {suggestion.currentValue or '_(not set)_'}")
            with col_now:
                st.caption("Text now")
                st.markdown(f"> {staleness.current_value or '_(not set)_'}")
        elif staleness.page_changed:
            st.caption("ℹ️ Other parts of this page changed since the suggestion was filed; this field did not.")

        if suggestion.suggestedValue:
            st.markdown("**Suggested text:**")
            st.markdown(f"> {suggestion.suggestedValue}")

        for comment in comments:
            comment_author = users.get(comment.userId, f"user {comment.userId}")
            when = comment.createdAt.strftime("%Y-%m-%d %H:%M")
            if comment.kind == "status_change":
                st.caption(f"_{comment.text}_ — {comment_author}, {when}")
            else:
                st.markdown(f"**{comment_author}** · {when}")
                st.markdown(comment.text)

        if user is None:
            st.caption("Sign-in not detected — reply/status controls disabled.")
            return

        reply_key = f"mrs_reply_{suggestion.id}"
        with st.form(key=f"{reply_key}_form", clear_on_submit=True, border=False):
            reply = st.text_area(
                "Reply", key=reply_key, height=80, label_visibility="collapsed", placeholder="Reply..."
            )
            col_send, col_status = st.columns([1, 3])
            with col_send:
                submitted = st.form_submit_button("Reply", use_container_width=True)
            if submitted and reply.strip():
                state.add_comment(suggestion.id, user.id, reply.strip())
                st.rerun()

        status = st.segmented_control(
            "Status",
            options=["open", "implemented", "rejected"],
            default=suggestion.status,
            key=f"mrs_status_{suggestion.id}",
            label_visibility="collapsed",
        )
        if status and status != suggestion.status:
            state.set_status(suggestion.id, user.id, status)
            st.rerun()


def _render_new_suggestion_control(field: ReviewableField, user: gm.User | None) -> None:
    if user is None:
        return
    with st.popover("✏️ Suggest a change / comment", use_container_width=False):
        with st.form(key=_key(field, "form"), clear_on_submit=True, border=False):
            suggested = st.text_area(
                "Suggested text",
                value=str(field.current_value) if field.current_value is not None else "",
                key=_key(field, "value"),
                height=120,
                help="Edit the text as you think it should read. Leave unchanged for a comment-only thread.",
            )
            comment = st.text_area(
                "Comment (why / context)",
                key=_key(field, "comment"),
                height=80,
                placeholder="Optional: explain the reasoning...",
            )
            if st.form_submit_button("Submit"):
                unchanged = suggested.strip() == str(field.current_value or "").strip()
                if unchanged and not comment.strip():
                    st.warning("Nothing to submit — edit the text or add a comment.")
                else:
                    state.create_suggestion(
                        field,
                        user_id=user.id,
                        suggested_value=None if unchanged else suggested.strip(),
                        comment_text=comment.strip() or None,
                    )
                    st.rerun()
