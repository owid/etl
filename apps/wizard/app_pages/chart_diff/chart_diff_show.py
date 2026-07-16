"""This module contains a main class `ChartDiffShow`, which handles all the visualisation aspect of chart diffs.


If you want to learn more about it, start from its `show` method.
"""

import difflib
import json
import os
from functools import cached_property
from typing import Any, cast

import pandas as pd
import streamlit as st
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher.model as gm
from apps.backport.datasync.data_metadata import (
    filter_out_fields_in_metadata_for_checksum,
)
from apps.chart_sync.admin_api import AdminAPI
from apps.utils.llms.gpt import OpenAIWrapper, get_cost_and_tokens
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff, ChartDiffsLoader
from apps.wizard.app_pages.chart_diff.citations import st_show_citations
from apps.wizard.app_pages.chart_diff.conflict_resolver import ChartDiffConflictResolver
from apps.wizard.app_pages.chart_diff.utils import ANALYTICS_NUM_DAYS, SOURCE, TARGET, prettify_date
from apps.wizard.utils.components import grapher_chart
from etl.config import OWID_ENV
from etl.grapher.io import variable_metadata_df_from_s3

log = get_logger()

# GPT model default
MODEL_DEFAULT = "gpt-5"

# How to display the various chart review statuses
DISPLAY_STATE_OPTIONS = {
    gm.ChartStatus.APPROVED.value: {
        "label": "Approve",
        "color": "green",
        "material_icon": ":material/thumb_up:",
        "icon": "✅",
    },
    gm.ChartStatus.PENDING.value: {
        "label": "Pending",
        "color": "gray",
        "material_icon": "",
        "icon": "⏳",
    },
    gm.ChartStatus.REJECTED.value: {
        "label": "Reject",
        "color": "red",
        "material_icon": ":material/thumb_down:",
        "icon": "❌",
    },
}
DISPLAY_STATE_OPTIONS_BINARY = {
    gm.ChartStatus.APPROVED.value: {
        "label": "Reviewed",
        "color": "green",
        "icon": "✅",
    },
    gm.ChartStatus.PENDING.value: {
        "label": "Unreviewed",
        "color": "gray",
        "icon": "⏳",
    },
}
# Help message if there is a conflict between production and staging (i.e. someone edited chart in production while we did on staging)
CONFLICT_HELP_MESSAGE = "The chart in production was modified after creating the staging server. Please resolve the conflict by integrating the latest changes from production into staging."


class ChartDiffShow:
    """Handle a chart-diff and show it.

    Showing a chart-diff involves showing various parts: the visualisation of the chart, the diff of the chart config, the history of approvals, and various controls.
    """

    def __init__(
        self,
        diff: ChartDiff,
        source_session: Session,
        target_session: Session,
        expander: bool = True,
        show_link: bool = True,
    ):
        self.diff = diff
        self.source_session = source_session
        self.target_session = target_session
        self.expander = expander
        self.show_link = show_link

        # OpenAI
        if "OPENAI_API_KEY" in os.environ:
            self.openai_api = OpenAIWrapper()
        else:
            self.openai_api = None

    @property
    def box_icon(self) -> str:
        """Icon of the expander box."""
        if self.diff.error:
            return "⚠️"
        return DISPLAY_STATE_OPTIONS[cast(str, self.diff.approval_status)]["icon"]

    @property
    def box_label(self):
        """Label of the expander box.

        This contains the state of the approval (by means of an emoji), the slug of the chart, and any tags (like "NEW" or "DRAFT").
        """
        label = f"{self.diff.slug}  "
        tags = []
        if self.diff.is_new:
            tags.append(" :green-badge[:material/grade: **NEW**]")
        if self.diff.is_draft:
            tags.append(" :gray-badge[:material/draft: **DRAFT**]")
        if self.diff.error:
            tags.append(" :red-badge[:material/error: **ERROR**]")
        for change in self.diff.change_types:
            tags.append(f":blue-badge[:material/commit: **{change.upper()} CHANGE**]")

        # Add TAG if modified and no change_types is provided
        if (self.diff.is_modified) and (tags == []):
            label += ":rainbow-badge[**UNKNOWN -- REPORT THIS**]"
        else:
            label += f"{' '.join(tags)}"
        return label

    @property
    def status_names(self) -> list[str]:
        """List with names of accepted statuses."""
        return list(DISPLAY_STATE_OPTIONS.keys())

    @property
    def status_names_binary(self) -> list[str]:
        """List with names of accepted statuses."""
        status = list(DISPLAY_STATE_OPTIONS.keys())
        status = [s for s in status if s not in {gm.ChartStatus.REJECTED.value}]
        return status

    @cached_property
    def approval_history(self) -> pd.DataFrame:
        """History of approvals of this chart diff, freshly loaded from the DB."""
        approvals = self.diff.get_all_approvals(self.source_session)
        return pd.DataFrame([{"updatedAt": a.updatedAt, "status": a.status} for a in approvals])

    @cached_property
    def last_approved_revision(self) -> gm.ChartRevisions | None:
        """Chart revision on staging at the time of the last approval (if any).

        Lets the reviewer compare the current staging chart against the version they last approved,
        instead of against production.
        """
        if self.diff.is_approved:
            return None
        df = self.approval_history
        if df.empty:
            return None
        approved = df[df["status"] == gm.ChartStatus.APPROVED.value]
        if approved.empty:
            return None
        try:
            return self.diff.get_last_chart_revision(self.source_session, approved["updatedAt"].max())
        except NoResultFound:
            # Chart has no revisions on staging (e.g. it was never edited via the admin).
            return None

    def _invalidate_history_cache(self) -> None:
        """Drop cached approval history so the next render reloads it from the DB."""
        self.__dict__.pop("approval_history", None)
        self.__dict__.pop("last_approved_revision", None)

    def _push_status(self, session: Session | None = None) -> None:
        """Change state of the ChartDiff based on session state."""
        if session is None:
            session = self.source_session
        status = st.session_state[f"status-ctrl-{self.diff.chart_id}"]
        # Deselecting the current option in the segmented control yields None; treat it as "pending".
        if status is None:
            status = gm.ChartStatus.PENDING.value
        self.diff.set_status(session=session, status=status)
        self.diff._clean_cache()
        self._invalidate_history_cache()
        # Store toast message in session state to display after fragment reruns
        # (displaying elements in fragment callbacks causes duplication bugs)
        st.session_state[f"toast-{self.diff.chart_id}"] = status

    def _refresh_chart_diff(self):
        """Get latest chart version from database."""
        diffs = ChartDiffsLoader(
            self.source_session.get_bind(),  # ty: ignore
            self.target_session.get_bind(),  # ty: ignore
            chart_ids=[self.diff.chart_id],
        ).get_diffs(config=True, data=True, metadata=True, tags=True)
        self._invalidate_history_cache()
        if diffs:
            diff_new = diffs[0]
            st.session_state.chart_diffs[self.diff.chart_id] = diff_new
            self.diff = diff_new
        else:
            # The chart no longer differs between environments (e.g. the change was reverted).
            st.session_state.chart_diffs.pop(self.diff.chart_id, None)
            st.session_state[f"chart-diff-gone-{self.diff.chart_id}"] = True

    @property
    def _header_production_chart(self):
        """Header for the production chart."""
        # Everything is fine
        if not self.diff.in_conflict:
            text_production = f"**Production** :material/event: {prettify_date(self.diff.target_chart)}"
        # Conflict with live
        else:
            text_production = f":red[**Production** :material/event: {prettify_date(self.diff.target_chart)}] ⚠️"

        return text_production

    @property
    def _header_production_chart_plain(self):
        """Header for the production chart."""
        # Everything is fine
        if not self.diff.in_conflict:
            text_production = f"Production ({prettify_date(self.diff.target_chart)})"
        # Conflict with live
        else:
            text_production = f"Production ({prettify_date(self.diff.target_chart)}) -- CONFLICT ⚠️"

        return text_production

    @property
    def _header_staging_chart(self):
        """Header for staging chart."""
        # Everything is fine
        if not self.diff.in_conflict:
            text_staging = f":green[**New version** :material/today: {prettify_date(self.diff.source_chart)}]"
        # Conflict with live
        else:
            text_staging = f"**New version** :material/today: {prettify_date(self.diff.source_chart)}"

        return text_staging

    def _show_conflict_resolver(self) -> None:
        """Resolve conflicts between charts in target and source.

        Sometimes, someone might edit a chart in production while we work on it on staging.
        """

        def _mark_as_resolved():
            self.diff.set_conflict_to_resolved(self.source_session)
            self._refresh_chart_diff()

        def _resolve_conflicts(resolver):
            resolver.resolve_conflicts(rerun=False)
            self._refresh_chart_diff()

        resolver = ChartDiffConflictResolver(self.diff, self.source_session)
        col1, col2 = st.columns(2)
        with col1:
            st.warning("This is under development! Find below a form with the different fields that present conflicts.")
        with col2:
            st.button(
                key=f"resolve-conflicts-{self.diff.chart_id}",
                label="⚠️ Mark as resolved: Accept all changes from staging",
                help="Click to resolve the conflict by accepting all changes from staging. The changes from production will be ignored. This can be useful if you're happy with the changes in staging as they are.",
                on_click=_mark_as_resolved,
            )

        # If things to compare...
        if resolver.config_compare:
            st.markdown(
                "Find below the chart config fields that do not match. Choose the value you want to keep for each of the fields (or introduce a new one)."
            )

            # Show conflict resolver per field
            ## Provide tools to merge the content of each field
            for field in resolver.config_compare:
                resolver._show_field_conflict_resolver(field)

            # Button to resolve all conflicts
            st.button(
                "Resolve conflicts",
                help="Click to resolve the conflicts and update the chart config.",
                key=f"resolve-conflicts-btn-{self.diff.chart_id}",
                type="primary",
                on_click=lambda r=resolver: _resolve_conflicts(r),
            )
        else:
            st.success(
                "No conflicts found actually. Unsure why you were prompted with the conflict resolver. Please report."
            )

    def _show_chart_diff_header(self):
        # Three columns: status, refresh, link
        col1, col2, col3 = st.columns([2, 3, 1], vertical_alignment="bottom")

        # Status of chart diff: approve, pending, reject
        with col1:
            if (
                self.diff.is_modified
                & ("config" not in self.diff.change_types)
                & (("data" in self.diff.change_types) | ("metadata" in self.diff.change_types))
            ):
                st.radio(
                    label="Did you review the chart?",
                    key=f"status-ctrl-{self.diff.chart_id}",
                    options=self.status_names_binary,
                    horizontal=True,
                    format_func=lambda x: (
                        f":{DISPLAY_STATE_OPTIONS_BINARY[x]['color']}-background[{DISPLAY_STATE_OPTIONS_BINARY[x]['label']}]"
                    ),
                    index=self.status_names_binary.index(self.diff.approval_status),  # ty: ignore
                    on_change=self._push_status,
                    help="Note that the changes in the chart come from ETL changes (metadata/data) and therefore there is no way to reject them at this stage. If you are not happy with the changes, please look at the ETL steps involved. We present them to you here as a sanity check, and ask you to review them for correctness.",
                )
            else:
                if self.diff.in_conflict:
                    help_text = "Resolve chart config conflicts before proceeding!"
                else:
                    help_text = (
                        "Charts need to be reviewed before merging your work, otherwise CI/CD will fail in your PR.\n\n"
                        "- **Approve chart**: After merging your PR, the chart in production will be updated with your edits.\n"
                        "- **Reject chart**: Your changes will be discarded and the chart in production will remain as is.\n"
                        "- **Pending**: You can come back later to approve or reject the chart.\n\n"
                        "Note that CI/CD will fail if any of the chart diffs is pending."
                    )

                def _format_status(x):
                    return f":{DISPLAY_STATE_OPTIONS[x]['color']}[{DISPLAY_STATE_OPTIONS[x]['material_icon']}] {DISPLAY_STATE_OPTIONS[x]['label']}"

                st.segmented_control(
                    label="Approve or reject chart",
                    key=f"status-ctrl-{self.diff.chart_id}",
                    options=self.status_names,
                    format_func=lambda x: _format_status(x),
                    default=self.diff.approval_status,  # ty: ignore
                    on_change=self._push_status,
                    disabled=self.diff.in_conflict,
                    help=help_text,
                    # label_visibility="collapsed",
                )

        if len(self.diff.article_refs) > 0:
            articles_md = "| Article Title | Daily Views |\n| --- | --- |\n" + "\n".join(
                [f"| [{art.title}]({art.url}) | {art.views_daily_pretty} |" for art in self.diff.article_refs]
            )
            articles_md = f"\n\n{articles_md}"
        else:
            articles_md = ""

        # Scores (analytics, anomalies, etc.)
        help_txt = (
            f":violet-badge[:material/auto_awesome: **Relevance**]: An attempt to measure the relative-importance of a specific chart diff. The more 'relevant' it is, the more time should be allocated to carefully review the chart changes. It is estimated by factoring in chart views, article views and anomaly scores. Draft charts have 0% relevance.\n\n"
            f":primary-badge[:material/remove_red_eye:] **Average number of daily chart views** in the last {ANALYTICS_NUM_DAYS} days.\n\n"
            f":primary-badge[:material/article:] **Number of articles** that use this chart.{articles_md}\n\n"
            ":primary-badge[:material/scatter_plot:] **Anomaly score of the chart**, as estimated by Anomalist. It is based on noticeable anomalies due to updating indicators in the chart. A score of 0% means that the chart doesn't have noticeable outliers (relative to the previous indicators), while a score closer to 100% means that there is an indicator with a substantial outlier.\n\n"
        )
        with col2:
            st.markdown(
                self.diff.scores.to_md(),
                help=help_txt,
            )

        # Actions: refresh + metadata diff
        with col3:
            st.button(
                label="Refresh",
                icon=":material/refresh:",
                key=f"refresh-btn-{self.diff.chart_id}",
                help="Get the latest version of the chart from the staging and production servers.",
                on_click=self._refresh_chart_diff,
                type="secondary",
            )
            if "metadata" in self.diff.change_types:
                # NOTE: opens a dialog (rather than a tab) so the S3 metadata is only fetched on demand
                if st.button(
                    "Metadata diff",
                    icon=":material/manage_search:",
                    key=f"btn-meta-diff-{self.diff.chart_id}",
                    help="Inspect the metadata changes of the indicators used in this chart.",
                ):
                    self._show_metadata_diff_modal()

    def _show_tags_if_changed(self, chart, session):
        """Show tags as gray badges if there are tag changes."""
        if "tags" in self.diff.change_types and chart:
            tags = chart.tags(session)
            if tags:
                badges = " ".join([f":gray-badge[{tag['name']}]" for tag in tags])
                st.markdown(badges)

    @st.dialog("Metadata differences", width="large")  # ty: ignore
    def _show_metadata_diff_modal(self) -> None:
        """Show metadata diff in a modal page."""
        # Sanity checks
        assert self.diff.is_modified, (
            "Metadata diff should only be shown for modified charts! Please report this issue."
        )
        assert self.diff.target_chart is not None, "Chart detected as modified but target_chart is None!"

        # Get indicator IDs from source & target
        source_ids = [x["variableId"] for x in self.diff.source_chart.config["dimensions"]]
        target_ids = [x["variableId"] for x in self.diff.target_chart.config["dimensions"]]

        # Only continue if IDs in prod and staging match!
        if set(source_ids) != set(target_ids):
            st.warning(
                f"List of indicators in source and target differs. Can't render this section.\n\nSOURCE: {source_ids}\n\nTARGET: {target_ids}"
            )
            return
        if not source_ids:
            return

        # Get metadata from S3 (results are aligned with source_ids)
        with st.spinner("Getting metadata from S3..."):
            metadata_source = variable_metadata_df_from_s3(source_ids, workers=10, env=SOURCE)
            metadata_target = variable_metadata_df_from_s3(source_ids, workers=10, env=TARGET)

        # Generate diffs
        meta_diffs = {}
        catalog_paths = {}
        for source, target, indicator_id in zip(metadata_source, metadata_target, source_ids):
            catalog_paths[indicator_id] = source.get("catalogPath", "")

            # Filter fields not relevant for comparison
            source = filter_out_fields_in_metadata_for_checksum(source)
            target = filter_out_fields_in_metadata_for_checksum(target)

            # PROD is the base; STAGING is what the user is proposing to merge — pass
            # `target` (prod) first so the diff reads as `production → staging` and
            # the staging branch shows up as additions/modifications.
            meta_diff = compare_dictionaries(target, source, fromfile="production", tofile="staging")
            if meta_diff:
                meta_diffs[indicator_id] = meta_diff

        if not meta_diffs:
            st.success("No differences found in the metadata fields relevant for comparison.")
            return

        # Placeholder for GPT summary (filled after the diffs render, so the user isn't blocked on the LLM)
        container = st.container()

        # Show diffs
        with st.expander("See complete diff", expanded=True):
            for indicator_id, meta_diff in meta_diffs.items():
                st.markdown(f"**Indicator ID: {indicator_id}**")
                if catalog_paths[indicator_id]:
                    st.caption(catalog_paths[indicator_id])
                st_show_diff(meta_diff)

        with container:
            self._show_metadata_diff_gpt_summary(meta_diffs)

    def _show_metadata_diff_gpt_summary(self, meta_diffs: dict[int, str]) -> None:
        """Summarise differences in metadata using GPT."""
        if self.openai_api is None:
            st.info("Set `OPENAI_API_KEY` in your `.env` to get an AI summary of the metadata changes.")
            return

        # Cache the summary in session state, keyed by the diff content, so re-opening the dialog
        # doesn't re-query the API but a changed diff does.
        # NOTE: Don't use `st.cache_data` here — caching a function that *renders* UI (and returns
        # None) makes the summary render once and then vanish on subsequent calls.
        cache_key = f"gpt-metadata-summary-{self.diff.chart_id}-{hash(tuple(sorted(meta_diffs.items())))}"
        cached = st.session_state.get(cache_key)

        with st.chat_message("assistant"):
            if cached is not None:
                response, cost_msg = cached
                st.markdown(response)
            else:
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You will be presented with the metadata diffs of various indicators, as a dictionary "
                            "mapping indicator ID to a unified diff (production → staging). Summarise the main "
                            "changes at a high level, in a few short bullet points. Group similar changes across "
                            "indicators, and ignore trivial ones (e.g. version bumps or ID changes)."
                        ),
                    },
                    {
                        "role": "user",
                        "content": str(meta_diffs),
                    },
                ]
                try:
                    stream = self.openai_api.chat.completions.create(
                        model=MODEL_DEFAULT,
                        messages=messages,  # ty: ignore
                        # NOTE: gpt-5 is a reasoning model and reasoning tokens count towards this
                        # limit; keep it well above the expected summary length or the visible
                        # output may be empty.
                        max_completion_tokens=4000,
                        reasoning_effort="low",
                        stream=True,
                    )
                    response = cast(str, st.write_stream(stream))
                except Exception as e:
                    st.error(f"AI summary failed: {e}")
                    return

                if not response:
                    st.warning("The model returned an empty summary.")
                    return

                # Cost information (estimated with tiktoken, hence lower bounds)
                text_in = "\n".join([m["content"] for m in messages])
                cost, num_tokens = get_cost_and_tokens(text_in, response, MODEL_DEFAULT)
                cost_msg = f"Cost: ≥{cost} USD. Tokens: ≥{num_tokens}."
                st.session_state[cache_key] = (response, cost_msg)
        st.caption(cost_msg)

    def _show_chart_comparison(self) -> tuple[Any, bool]:
        """Show charts (horizontally or vertically)."""

        def _show_chart_old():
            last_approved_revision = self.last_approved_revision
            if (last_approved_revision is not None) and (not self.diff.in_conflict):
                with st.container(height=40, border=False):
                    options = {
                        "prod": self._header_production_chart_plain,
                        "last": f"Last approved on staging ({prettify_date(last_approved_revision)} - REV {last_approved_revision.id})",
                    }
                    option = st.selectbox(
                        "Chart revision",
                        options=options.keys(),
                        format_func=lambda x: options[x],
                        key=f"prod-review-{self.diff.chart_id}",
                        label_visibility="collapsed",
                    )

                if option == "prod":
                    self._show_tags_if_changed(self.diff.target_chart, self.target_session)
                    assert self.diff.target_chart is not None
                    grapher_chart(chart_config=self.diff.target_chart.config, owid_env=TARGET)
                elif option == "last":
                    grapher_chart(chart_config=last_approved_revision.config, owid_env=SOURCE)
                    return last_approved_revision.config, False
            else:
                if self.diff.in_conflict:
                    st.markdown(self._header_production_chart, help=CONFLICT_HELP_MESSAGE)
                else:
                    st.markdown(self._header_production_chart)

                self._show_tags_if_changed(self.diff.target_chart, self.target_session)

                assert self.diff.target_chart is not None
                grapher_chart(chart_config=self.diff.target_chart.config, owid_env=TARGET)

            assert self.diff.target_chart is not None
            return self.diff.target_chart.config, True

        def _show_chart_new():
            if self.last_approved_revision is None:
                st.markdown(self._header_staging_chart)
            else:
                with st.container(height=40, border=False):
                    st.markdown(self._header_staging_chart)

            self._show_tags_if_changed(self.diff.source_chart, self.source_session)

            grapher_chart(chart_config=self.diff.source_chart.config, owid_env=SOURCE)

        def _show_charts_comparison_v() -> tuple[Any, bool]:
            """Show charts on top of each other."""
            # Chart production
            config_ref, is_prod = _show_chart_old()

            # Chart staging
            _show_chart_new()

            return config_ref, is_prod

        def _show_charts_comparison_h() -> tuple[Any, bool]:
            """Show charts next to each other."""
            # Create two columns for the iframes
            col1, col2 = st.columns(2)

            with col1:
                config_ref, is_prod = _show_chart_old()
            with col2:
                _show_chart_new()
            return config_ref, is_prod

        # Only one chart: new chart
        is_prod = True
        if self.diff.target_chart is None:
            st.markdown(f"New version ┃ _{prettify_date(self.diff.source_chart)}_")
            grapher_chart(chart_config=self.diff.source_chart.config, owid_env=SOURCE)
            config_ref = self.diff.source_chart.config
        # Two charts, actual diff
        else:
            # Detect arrangement type
            arrange_vertical = st.session_state.get(
                f"arrange-charts-vertically-{self.diff.chart_id}", False
            ) | st.session_state.get("arrange-charts-vertically", False)

            # Show charts
            if arrange_vertical:
                config_ref, is_prod = _show_charts_comparison_v()
            else:
                config_ref, is_prod = _show_charts_comparison_h()

            # Enable/disable vertical arrangement
            st.toggle(
                "Arrange charts vertically",
                key=f"arrange-charts-vertically-{self.diff.chart_id}",
            )

        return config_ref, is_prod

    def _show_config_diff(self, config_ref, fromfile: str = "production") -> None:
        assert self.diff.target_chart is not None, (
            "We detected this diff to be a chart modification, but couldn't find target chart!"
        )

        # config_1 = self.diff.target_chart.config
        config_2 = self.diff.source_chart.config

        _show_dict_diff(config_ref, config_2, fromfile=fromfile)

    def _show_approval_history(self):
        """Show history of approvals of a chart-diff."""
        df = self.approval_history
        if df.empty:
            st.markdown("No approval history found!")
            return

        df = df.sort_values("updatedAt", ascending=False)
        df["status"] = df["status"].apply(lambda x: f"{DISPLAY_STATE_OPTIONS[str(x)]['icon']} {x}")
        st.dataframe(
            df,
            column_order=["updatedAt", "status"],
            column_config={
                "updatedAt": st.column_config.DatetimeColumn(
                    "Updated",
                    format="D MMM YYYY, hh:mm:ss",
                    step=60,
                ),
                "status": st.column_config.Column(
                    "Status",
                ),
            },
            hide_index=True,
        )

    def _show_narrative_charts(self) -> None:
        """Show narrative charts that use this chart as parent, with side-by-side comparison."""
        if not self.diff.chart_id:
            return

        # Respect the sidebar toggle. The list-side filter in app.py only runs when
        # `show_all` is off; without this gate the block would still render here when
        # the user toggles narrative-charts off but also has "Show all charts" enabled.
        # Mirrors the same pattern in `_show_citations`.
        if not st.session_state.get("show-narrative-charts", True):
            return

        # Load narrative charts for this parent chart
        narrative_charts = gm.NarrativeChart.load_narrative_charts_by_parent_chart_ids(
            self.source_session, {self.diff.chart_id}
        )

        if not narrative_charts:
            return

        st.markdown("##### 📖 Narrative Charts")

        # Get narrative chart configs from both environments
        source_api = AdminAPI(SOURCE)
        target_api = AdminAPI(TARGET)

        for nc in narrative_charts:
            with st.container(border=True):
                source_admin_url = f"{SOURCE.admin_site}/narrative-charts/{nc.id}/edit"
                target_admin_url = f"{TARGET.admin_site}/narrative-charts/{nc.id}/edit"

                # Get full configs from both environments via API
                try:
                    source_nc = source_api.get_narrative_chart(nc.id)
                    source_config = source_nc.get("configFull", {})
                except Exception as e:
                    log.warning(f"_show_narrative_charts: source failed nc.id={nc.id}: {e}")
                    source_config = None

                try:
                    target_nc = target_api.get_narrative_chart(nc.id)
                    target_config = target_nc.get("configFull", {})
                except Exception as e:
                    log.warning(f"_show_narrative_charts: target failed nc.id={nc.id}: {e}")
                    target_config = None

                # Show side-by-side comparison
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown(f"[Production]({target_admin_url})")
                    if target_config:
                        grapher_chart(chart_config=target_config, owid_env=TARGET)
                    else:
                        st.info("Not available in production")

                with col2:
                    st.markdown(f"[Staging]({source_admin_url})")
                    if source_config:
                        grapher_chart(chart_config=source_config, owid_env=SOURCE)
                    else:
                        st.info("Not available in staging")

    def _show_citations(self) -> None:
        """Show articles that cite this chart with scroll-to-text fragment URLs."""
        if not st.session_state.get("show-article-citations", True):
            return
        st_show_citations(
            self.diff.slug,
            self.source_session,
            self.target_session,
            SOURCE,
            TARGET,
        )

    def _show(self) -> None:
        """Show chart diff.

        The diff consists of multiple views, depending on whether the chart is a modification or is new.

        Some views include: chart side-to-side, config diff, approval history.

        If a conflict is detected (i.e. edits in production), a conflict resolver is shown.
        """
        if self.diff.in_conflict:
            with st.popover("⚠️ Resolve conflict"):
                self._show_conflict_resolver()

        if self.diff.error:
            st.error(f"⚠️ Error: {self.diff.error}")

        # Show header: approval/reject controls, scores, action buttons
        self._show_chart_diff_header()

        # SHOW MODIFIED CHART
        if self.diff.is_modified:
            tab1, tab2, tab3 = st.tabs(["Charts", "Config diff", "Status log"])
            with tab1:
                config_ref, is_prod = self._show_chart_comparison()
            with tab2:
                self._show_config_diff(config_ref, "production" if is_prod else "last revision")
            with tab3:
                self._show_approval_history()

        # SHOW NEW CHART
        elif self.diff.is_new:
            tab1, tab2 = st.tabs(["Chart", "Status log"])
            with tab1:
                _ = self._show_chart_comparison()
            with tab2:
                self._show_approval_history()

        # SHOW NARRATIVE CHARTS
        self._show_narrative_charts()

        # SHOW ARTICLE CITATIONS
        self._show_citations()

        # Copy link
        if self.show_link:
            query_params = f"chart_id={self.diff.chart_id}&show_reviewed="
            if OWID_ENV.wizard_url != OWID_ENV.wizard_url_remote:
                url = f"{OWID_ENV.wizard_url_remote}/chart-diff?{query_params}"
                st.caption(
                    body=f":material/link: {url}",
                    help=f"Shown is the link to the remote chart-diff.\n\n Alternatively, local link: {OWID_ENV.wizard_url}?{query_params}",
                )
            else:
                url = f"{OWID_ENV.wizard_url}/chart-diff?{query_params}"
                st.caption(body=f":material/link: {url}")

    def _show_deferred_toast(self) -> None:
        """Show toast message if one was queued by a status change callback."""
        toast_key = f"toast-{self.diff.chart_id}"
        if toast_key in st.session_state:
            status = st.session_state.pop(toast_key)
            match status:
                case gm.ChartStatus.APPROVED.value:
                    st.toast(f":green[Chart {self.diff.chart_id} has been **approved**]", icon="✅")
                case gm.ChartStatus.REJECTED.value:
                    st.toast(f":red[Chart {self.diff.chart_id} has been **rejected**]", icon="❌")
                case gm.ChartStatus.PENDING.value:
                    st.toast(f"**Resetting** state for chart {self.diff.chart_id}.", icon=":material/restart_alt:")

    @st.fragment
    def show(self):
        """Show chart diff."""
        # Chart diff no longer exists (e.g. after a refresh found no differences)
        if st.session_state.pop(f"chart-diff-gone-{self.diff.chart_id}", False):
            st.info(
                f"Chart {self.diff.chart_id} no longer differs between staging and production. "
                "Reload the page to update the list."
            )
            return

        # Show deferred toast from previous status change (must be outside callback to avoid duplication bug)
        self._show_deferred_toast()

        # Show in expander or not
        if self.expander:
            with st.expander(
                label=self.box_label,
                icon=self.box_icon,
                expanded=not self.diff.is_reviewed,
            ):
                self._show()
        else:
            self._show()


def compare_strings(s1: str, s2: str, fromfile: str, tofile: str = "staging"):
    """Get diff of two multi-line strings.

    Useful for chart config diffs, indicator metadata diffs, etc.
    """
    diff = difflib.unified_diff(
        s1.strip().splitlines(keepends=True),
        s2.strip().splitlines(keepends=True),
        fromfile=fromfile,
        tofile=tofile,
    )

    diff_string = "".join(diff)

    return diff_string


def compare_dictionaries(dix_1: dict[str, Any], dix_2: dict[str, Any], fromfile: str, tofile: str = "staging"):
    """Get diff of two dictionaries.

    Useful for chart config diffs, indicator metadata diffs, etc.
    """
    return compare_strings(json.dumps(dix_1, indent=4), json.dumps(dix_2, indent=4), fromfile=fromfile, tofile=tofile)


def st_show_diff(diff_str, **kwargs):
    """Display diff."""
    st.code(diff_str, line_numbers=True, language="diff", **kwargs)


def _show_dict_diff(dix_1: dict[str, Any], dix_2: dict[str, Any], fromfile: str):
    """Show diff of two dictionaries.

    Used to show chart config diffs, indicator metadata diffs, etc.
    """
    diff_str = compare_dictionaries(dix_1, dix_2, fromfile=fromfile)
    st_show_diff(diff_str)


def st_show(
    diff: ChartDiff,
    source_session: Session,
    target_session: Session,
    expander: bool = True,
    show_link: bool = True,
) -> None:
    """Show the chart diff in Streamlit."""
    handle = ChartDiffShow(
        diff=diff,
        source_session=source_session,
        target_session=target_session,
        expander=expander,
        show_link=show_link,
    )
    handle.show()
