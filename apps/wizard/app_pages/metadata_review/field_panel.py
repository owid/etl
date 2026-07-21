"""The field row: provenance badge, current text, one consolidated proposal, and controls.

A field carries at most one OPEN proposal (enforced by
`MetadataReviewSuggestion.file_or_update`): later reviewers refine the same
proposed text or join its thread, instead of stacking parallel threads that each
repeat the field's content. For `description_key` the proposal renders as a
bullet-level diff, so only the changed bullets show.
"""

import hashlib

import streamlit as st

import etl.grapher.model as gm
from apps.metadata_review.diffs import bullet_diff, diff_markdown_lines, diff_summary
from apps.metadata_review.resolution import Staleness, check_staleness
from apps.metadata_review.targets import MdimReview, ReviewableField
from apps.wizard.app_pages.metadata_review import state

PROVENANCE_BADGES = {
    "override": ("green", "set in the MDim config"),
    "inherited": ("blue", "inherited from the indicator (garden metadata)"),
    "missing": ("red", "not set anywhere"),
}
STATUS_ICONS = {"open": "💬", "implemented": "✅", "rejected": "🚫"}

DESCRIPTION_KEY_FIELDS = {"metadata.description_key", "description_key"}


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
    """Render one reviewable field with its consolidated proposal and history."""
    color, explanation = PROVENANCE_BADGES[field.provenance]
    open_suggestions = [s for s in suggestions if s.status == "open"]
    resolved = [s for s in suggestions if s.status != "open"]
    # At most one open proposal per field going forward; render the newest as
    # canonical if legacy parallel threads exist.
    open_suggestions.sort(key=lambda s: s.createdAt, reverse=True)
    proposal = open_suggestions[0] if open_suggestions else None

    with st.container(border=True):
        title_col, badge_col = st.columns([4, 2], vertical_alignment="center")
        with title_col:
            st.markdown(f"**{field.label}**")
        with badge_col:
            badges = [f":{color}-badge[{field.provenance}]"]
            if proposal is not None:
                badges.append(":orange-badge[open proposal]")
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

        if proposal is not None:
            _render_proposal(
                field,
                proposal,
                extra_threads=open_suggestions[1:],
                comments_by_suggestion=comments_by_suggestion,
                users=users,
                user=user,
                fields_by_key=fields_by_key,
            )
        elif user is not None:
            _render_suggestion_form(field, user, existing=None)

        for suggestion in resolved:
            _render_resolved(suggestion, comments_by_suggestion.get(suggestion.id, []), users, user)


def _render_proposal(
    field: ReviewableField,
    proposal: gm.MetadataReviewSuggestion,
    extra_threads: list[gm.MetadataReviewSuggestion],
    comments_by_suggestion: dict[int, list],
    users: dict[int, str],
    user: gm.User | None,
    fields_by_key: dict[tuple, ReviewableField],
) -> None:
    """The single consolidated proposal block for a field."""
    staleness: Staleness = check_staleness(proposal, fields_by_key)
    author = users.get(proposal.createdBy, f"user {proposal.createdBy}")

    with st.container(border=True):
        header = f"**✏️ Proposed change** — started by {author}, {proposal.createdAt:%Y-%m-%d}"
        if staleness.is_stale:
            header += " · :orange-badge[⚠️ page changed since]"
        st.markdown(header)

        if staleness.target_gone:
            st.warning("The view/indicator this proposal was filed on no longer exists on this page.")
        elif staleness.field_changed:
            st.warning(
                "The field's text changed since this proposal was filed — "
                "re-check the proposal against the current text above."
            )
            with st.expander("Text when the proposal was filed", expanded=False):
                st.markdown(f"> {proposal.currentValue or '_(not set)_'}")

        if proposal.suggestedValue is None:
            st.caption("_Discussion only — no replacement text proposed yet._")
        elif field.field_path in DESCRIPTION_KEY_FIELDS:
            ops = bullet_diff(field.current_value, proposal.suggestedValue)
            st.caption(f"Bullet changes ({diff_summary(ops)}):")
            st.markdown("\n".join(diff_markdown_lines(ops)))
        else:
            st.markdown(f"> {proposal.suggestedValue}")

        # Thread: comments from the canonical proposal plus any legacy parallel threads.
        comments = list(comments_by_suggestion.get(proposal.id, []))
        for legacy in extra_threads:
            comments += comments_by_suggestion.get(legacy.id, [])
        comments.sort(key=lambda c: c.createdAt)
        for comment in comments:
            comment_author = users.get(comment.userId, f"user {comment.userId}")
            when = comment.createdAt.strftime("%Y-%m-%d %H:%M")
            if comment.kind in ("status_change", "revision"):
                st.caption(f"_{comment.text}_ — {comment_author}, {when}")
            else:
                st.markdown(f"**{comment_author}** · {when}")
                st.markdown(comment.text)

        if user is None:
            st.caption("Sign-in not detected — reply/status controls disabled.")
            return

        reply_key = f"mrs_reply_{proposal.id}"
        with st.form(key=f"{reply_key}_form", clear_on_submit=True, border=False):
            reply = st.text_area(
                "Reply", key=reply_key, height=80, label_visibility="collapsed", placeholder="Reply..."
            )
            if st.form_submit_button("Reply") and reply.strip():
                state.add_comment(proposal.id, user.id, reply.strip())
                st.rerun()

        control_col, edit_col = st.columns([3, 2], vertical_alignment="center")
        with control_col:
            status = st.segmented_control(
                "Status",
                options=["open", "implemented", "rejected"],
                default=proposal.status,
                key=f"mrs_status_{proposal.id}",
                label_visibility="collapsed",
            )
            if status and status != proposal.status:
                state.set_status(proposal.id, user.id, status)
                st.rerun()
        with edit_col:
            _render_suggestion_form(field, user, existing=proposal)


def _render_resolved(
    suggestion: gm.MetadataReviewSuggestion,
    comments: list,
    users: dict[int, str],
    user: gm.User | None,
) -> None:
    """Resolved proposals collapse to a single history line."""
    icon = STATUS_ICONS.get(suggestion.status, "💬")
    author = users.get(suggestion.createdBy, f"user {suggestion.createdBy}")
    with st.expander(
        f"{icon} {suggestion.status} — proposal by {author}, {suggestion.createdAt:%Y-%m-%d}", expanded=False
    ):
        if suggestion.suggestedValue:
            st.markdown(f"> {suggestion.suggestedValue}")
        for comment in comments:
            comment_author = users.get(comment.userId, f"user {comment.userId}")
            when = comment.createdAt.strftime("%Y-%m-%d %H:%M")
            if comment.kind in ("status_change", "revision"):
                st.caption(f"_{comment.text}_ — {comment_author}, {when}")
            else:
                st.markdown(f"**{comment_author}** · {when}: {comment.text}")
        if user is not None:
            if st.button("Reopen", key=f"mrs_reopen_{suggestion.id}"):
                state.set_status(suggestion.id, user.id, "open")
                st.rerun()


def _render_suggestion_form(
    field: ReviewableField,
    user: gm.User,
    existing: gm.MetadataReviewSuggestion | None,
) -> None:
    """Popover to start a proposal, or refine the field's existing one."""
    label = "✏️ Edit proposed text" if existing is not None else "✏️ Suggest a change / comment"
    prefill = (
        existing.suggestedValue
        if existing is not None and existing.suggestedValue is not None
        else (str(field.current_value) if field.current_value is not None else "")
    )
    with st.popover(label, use_container_width=False):
        with st.form(key=_key(field, "form"), clear_on_submit=True, border=False):
            suggested = st.text_area(
                "Proposed text",
                value=prefill,
                key=_key(field, "value"),
                height=120,
                help="Edit the text as you think it should read. Leave unchanged for a comment-only entry.",
            )
            comment = st.text_area(
                "Comment (why / context)",
                key=_key(field, "comment"),
                height=80,
                placeholder="Optional: explain the reasoning...",
            )
            if st.form_submit_button("Submit"):
                unchanged = suggested.strip() == prefill.strip()
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
