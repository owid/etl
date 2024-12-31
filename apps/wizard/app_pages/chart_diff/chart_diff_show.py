"""This module contains a main class `ChartDiffShow`, which handles all the visualisation aspect of chart diffs.


If you want to learn more about it, start from its `show` method.
"""

import difflib
import json
import os
from typing import Any, Dict, List, Optional, cast

import streamlit as st
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.backport.datasync.data_metadata import (
    filter_out_fields_in_metadata_for_checksum,
)
from apps.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff, ChartDiffsLoader
from apps.wizard.app_pages.chart_diff.conflict_resolver import ChartDiffConflictResolver
from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET, prettify_date
from apps.wizard.utils.components import grapher_chart
from etl.config import OWID_ENV
from etl.grapher.io import variable_metadata_df_from_s3

# How to display the various chart review statuses
DISPLAY_STATE_OPTIONS = {
    gm.ChartStatus.APPROVED.value: {
        "label": "Approve",
        "color": "green",
        # "icon": ":material/done_outline:",
        "icon": "✅",
    },
    gm.ChartStatus.REJECTED.value: {
        "label": "Reject",
        "color": "red",
        # "icon": ":material/delete:",
        "icon": "❌",
    },
    gm.ChartStatus.PENDING.value: {
        "label": "Pending",
        "color": "gray",
        # "icon": ":material/schedule:",
        "icon": "⏳",
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
            tags.append(" :blue-background[:material/grade: **NEW**]")
        if self.diff.is_draft:
            tags.append(" :gray-background[:material/draft: **DRAFT**]")
        if self.diff.error:
            tags.append(" :red-background[:material/error: **ERROR**]")
        for change in self.diff.change_types:
            tags.append(f":red-background[:material/refresh: **{change.upper()} CHANGE**]")

        # Add TAG if modified and no change_types is provided
        if (self.diff.is_modified) and (tags == []):
            label += ":rainbow-background[**UNKNOWN -- REPORT THIS**]"
        else:
            label += f"{' '.join(tags)}"
        return label

    @property
    def status_names(self) -> List[str]:
        """List with names of accepted statuses."""
        return list(DISPLAY_STATE_OPTIONS.keys())

    @property
    def status_names_binary(self) -> List[str]:
        """List with names of accepted statuses."""
        status = list(DISPLAY_STATE_OPTIONS.keys())
        status = [s for s in status if s not in {gm.ChartStatus.REJECTED.value}]
        return status

    def _push_status(self, session: Optional[Session] = None) -> None:
        """Change state of the ChartDiff based on session state."""
        if session is None:
            session = self.source_session
        with st.spinner():
            status = st.session_state[f"radio-{self.diff.chart_id}"]
            self.diff.set_status(session=session, status=status)

            # Notify user
            match status:
                case gm.ChartStatus.APPROVED.value:
                    st.toast(f":green[Chart {self.diff.chart_id} has been **approved**]", icon="✅")
                case gm.ChartStatus.REJECTED.value:
                    st.toast(f":red[Chart {self.diff.chart_id} has been **rejected**]", icon="❌")
                case gm.ChartStatus.PENDING.value:
                    st.toast(f"**Resetting** state for chart {self.diff.chart_id}.", icon=":material/restart_alt:")
        self.diff._clean_cache()

    def _push_status_binary(self, session: Optional[Session] = None) -> None:
        """Change state of the ChartDiff based on session state."""
        if session is None:
            session = self.source_session
        with st.spinner():
            status = st.session_state[f"radio-{self.diff.chart_id}"]
            self.diff.set_status(session=session, status=status)

            # Notify user
            match status:
                case gm.ChartStatus.APPROVED.value:
                    st.toast(f":green[Chart {self.diff.chart_id} has been **reviewed**]", icon="✅")
                case gm.ChartStatus.PENDING.value:
                    st.toast(f"**Resetting** state for chart {self.diff.chart_id}.", icon=":material/restart_alt:")
        self.diff._clean_cache()

    def _refresh_chart_diff(self):
        """Get latest chart version from database."""
        diff_new = ChartDiffsLoader(self.source_session.get_bind(), self.target_session.get_bind()).get_diffs(  # type: ignore
            sync=True, chart_ids=[self.diff.chart_id]
        )[0]
        st.session_state.chart_diffs[self.diff.chart_id] = diff_new
        self.diff = diff_new

    @property
    def _header_production_chart(self):
        """Header for the production chart."""
        # Everything is fine
        if not self.diff.in_conflict:
            text_production = f"Production ┃ _{prettify_date(self.diff.target_chart)}_"
        # Conflict with live
        else:
            text_production = f":red[Production ┃ _{prettify_date(self.diff.target_chart)}_] ⚠️"

        return text_production

    @property
    def _header_staging_chart(self):
        """Header for staging chart."""
        # Everything is fine
        if not self.diff.in_conflict:
            text_staging = f":green[New version ┃ _{prettify_date(self.diff.source_chart)}_]"
        # Conflict with live
        else:
            text_staging = f"New version ┃ _{prettify_date(self.diff.source_chart)}_"

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
                help="Click to resolve the conflict by accepting all changes from staging. The changes from production will be ignored. This can be useul if you're happy with the changes in staging as they are.",
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

    def _show_chart_diff_controls(self):
        # Three columns: status, refresh, link
        col1, col2, col3 = st.columns([2, 1, 3])

        # Status of chart diff: approve, pending, reject
        with col1:
            if (
                self.diff.is_modified
                & ("config" not in self.diff.change_types)
                & (("data" in self.diff.change_types) | ("metadata" in self.diff.change_types))
            ):
                st.radio(
                    label="Did you review the chart?",
                    key=f"radio-{self.diff.chart_id}",
                    options=self.status_names_binary,
                    horizontal=True,
                    format_func=lambda x: f":{DISPLAY_STATE_OPTIONS_BINARY[x]['color']}-background[{DISPLAY_STATE_OPTIONS_BINARY[x]['label']}]",
                    index=self.status_names_binary.index(self.diff.approval_status),  # type: ignore
                    on_change=self._push_status_binary,
                    help="Note that the changes in the chart come from ETL changes (metadata/data) and therefore there is no way to reject them at this stage. If you are not happy with the changes, please look at the ETL steps involved. We present them to you here as a sanity check, and ask you to review them for correctness.",
                )
            else:
                if self.diff.in_conflict:
                    help_text = "Resolve chart config conflicts before proceeding!"
                else:
                    help_text = "Approve or reject the chart. If you are not sure, please leave it as pending."
                st.radio(
                    label="Approve or reject chart",
                    key=f"radio-{self.diff.chart_id}",
                    options=self.status_names,
                    horizontal=True,
                    format_func=lambda x: f":{DISPLAY_STATE_OPTIONS[x]['color']}-background[{DISPLAY_STATE_OPTIONS[x]['label']}]",
                    index=self.status_names.index(self.diff.approval_status),  # type: ignore
                    on_change=self._push_status,
                    disabled=self.diff.in_conflict,
                    help=help_text,
                )

        # Refresh chart
        with col2:
            st.button(
                label="🔄 Refresh",
                key=f"refresh-btn-{self.diff.chart_id}",
                help="Get the latest version of the chart from the staging server.",
                on_click=self._refresh_chart_diff,
            )
        # Copy link
        if self.show_link:
            with col3:
                query_params = f"chart_id={self.diff.chart_id}"
                # st.caption(f"**{OWID_ENV.wizard_url}?{query_params}**")
                if OWID_ENV.wizard_url != OWID_ENV.wizard_url_remote:
                    st.caption(
                        f"**{OWID_ENV.wizard_url_remote}/chart-diff?{query_params}**",
                        help=f"Shown is the link to the remote chart-diff.\n\n Alternatively, local link: {OWID_ENV.wizard_url}?{query_params}",
                    )
                else:
                    st.caption(f"**{OWID_ENV.wizard_url}/chart-diff?{query_params}**")

    def _show_metadata_diff(self) -> None:
        """Show metadata diff (if applicable).

        Come chart-diffs might be triggered by changes in metadata. This allows the user to explore this changes.

        Note that to access the metadata, one needs to retrieve the JSON metadata files from the S3 bucket.
        """
        if st.button(
            "🔎 Metadata differences",
            f"btn-meta-diff-{self.diff.chart_id}",
        ):
            self._show_metadata_diff_modal()

    @st.dialog("Metadata differences", width="large")  # type: ignore
    def _show_metadata_diff_modal(self) -> None:
        """Show metadata diff in a modal page."""
        # Sanity checks
        assert (
            self.diff.is_modified
        ), "Metadata diff should only be shown for modified charts! Please report this issue."
        assert self.diff.target_chart is not None, "Chart detected as modified but target_chart is None!"

        # Get indicator IDs from source & target
        source_ids = [x["variableId"] for x in self.diff.source_chart.config["dimensions"]]
        target_ids = [x["variableId"] for x in self.diff.target_chart.config["dimensions"]]

        # Only continue if IDs in prod and staging match!
        if set(source_ids) != set(target_ids):
            st.warning(
                f"List of indicators in source and target differs. Can't render this section.\n\nSOURCE: {source_ids}\n\nTARGET: {target_ids}"
            )
        elif source_ids:
            # Get metadata from S3
            with st.spinner("Getting metadata from S3..."):
                # Get metadata from source & target
                metadata_source = variable_metadata_df_from_s3(source_ids, env=SOURCE)
                metadata_target = variable_metadata_df_from_s3(source_ids, env=TARGET)

            # Generate diffs
            meta_diffs = {}
            for source, target, indicator_id in zip(metadata_source, metadata_target, source_ids):
                # Filter fields not relevant for comparison
                source = filter_out_fields_in_metadata_for_checksum(source)
                target = filter_out_fields_in_metadata_for_checksum(target)

                # Get meta json diff
                meta_diff = compare_dictionaries(source, target)  # type: ignore
                if meta_diff:
                    meta_diffs[indicator_id] = meta_diff

            # Placeholder for GPT summary
            container = st.container()

            # Show diffs
            with st.expander("See complete diff", expanded=True):
                for (indicator_id, meta_diff), source in zip(meta_diffs.items(), metadata_source):
                    st.markdown(f"**Indicator ID: {indicator_id}**")
                    if ("catalogPath" in source) and (source["catalogPath"] != ""):
                        st.caption(source["catalogPath"])
                    st_show_diff(meta_diff)

            with container:
                self._show_metadata_diff_gpt_summary(meta_diffs)

    @st.cache_data(show_spinner=False)
    def _show_metadata_diff_gpt_summary(_self, meta_diffs) -> None:
        """Summarise differences in metadata using GPT."""
        if _self.openai_api is not None:
            api = OpenAIWrapper()
            with st.chat_message("assistant"):
                # Ask GPT (stream)
                messages = [
                    {
                        "role": "system",
                        "content": "You will be presented with the diffs of various indicator config files. Please summarise at a high-level what the main differences are. The diffs are given by means of a dictionary, with key (indicator ID) and value (indicator config diff).",
                    },
                    {
                        "role": "user",
                        "content": str(meta_diffs),
                    },
                ]
                stream = api.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,  # type: ignore
                    temperature=0.15,
                    max_tokens=1000,
                    stream=True,
                )
                response = cast(str, st.write_stream(stream))

            # Print cost information
            text_in = "\n".join([m["content"] for m in messages])
            cost, num_tokens = get_cost_and_tokens(text_in, response, "gpt-4o")
            cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
            st.info(cost_msg)

    def _show_chart_comparison(self) -> None:
        """Show charts (horizontally or vertically)."""

        def _show_charts_comparison_v():
            """Show charts on top of each other."""
            # Chart production
            if self.diff.in_conflict:
                help_text = CONFLICT_HELP_MESSAGE
                st.markdown(self._header_production_chart, help=help_text)
            else:
                st.markdown(self._header_production_chart)
            assert self.diff.target_chart is not None
            grapher_chart(chart_config=self.diff.target_chart.config, owid_env=TARGET)

            # Chart staging
            st.markdown(self._header_staging_chart)
            grapher_chart(chart_config=self.diff.source_chart.config, owid_env=SOURCE)

        def _show_charts_comparison_h():
            """Show charts next to each other."""
            # Create two columns for the iframes
            col1, col2 = st.columns(2)

            with col1:
                if self.diff.in_conflict:
                    help_text = CONFLICT_HELP_MESSAGE
                    st.markdown(self._header_production_chart, help=help_text)
                else:
                    st.markdown(self._header_production_chart)
                assert self.diff.target_chart is not None
                grapher_chart(chart_config=self.diff.target_chart.config, owid_env=TARGET)
            with col2:
                st.markdown(self._header_staging_chart)
                grapher_chart(chart_config=self.diff.source_chart.config, owid_env=SOURCE)

        # Only one chart: new chart
        if self.diff.target_chart is None:
            st.markdown(f"New version ┃ _{prettify_date(self.diff.source_chart)}_")
            grapher_chart(chart_config=self.diff.source_chart.config, owid_env=SOURCE)
        # Two charts, actual diff
        else:
            # Detect arrangement type
            arrange_vertical = st.session_state.get(
                f"arrange-charts-vertically-{self.diff.chart_id}", False
            ) | st.session_state.get("arrange-charts-vertically", False)

            # Show charts
            if arrange_vertical:
                _show_charts_comparison_v()
            else:
                _show_charts_comparison_h()

            # Enable/disable vertical arrangement
            st.toggle(
                "Arrange charts vertically",
                key=f"arrange-charts-vertically-{self.diff.chart_id}",
                # on_change=None,
            )

    def _show_config_diff(self) -> None:
        assert (
            self.diff.target_chart is not None
        ), "We detected this diff to be a chart modification, but couldn't find target chart!"

        config_1 = self.diff.target_chart.config
        config_2 = self.diff.source_chart.config

        _show_dict_diff(config_1, config_2)

    def _show_approval_history(self):
        """Show history of approvals of a chart-diff."""
        approvals = self.diff.get_all_approvals(self.source_session)
        # Get text
        text = ""
        for counter, approval in enumerate(approvals):
            emoji = DISPLAY_STATE_OPTIONS[str(approval.status)]["icon"]
            color = DISPLAY_STATE_OPTIONS[str(approval.status)]["color"]
            text_ = f"{approval.updatedAt}: {emoji} :{color}[{approval.status}]"

            if counter == 0:
                text_ = f"**{text_}**"

            text += text_ + "\n\n"

        st.markdown(text)

    def _show(self) -> None:
        """Show chart diff.

        The diff consists of multiple views, depending on whether the chart is a modification or is new.

        Some views include: chart side-to-side, config diff, approval history.

        If a conflict is detected (i.e. edits in production), a conflict resolver is shown.
        """
        if self.diff.in_conflict:
            with st.popover("⚠️ Resolve conflict"):
                self._show_conflict_resolver()
        else:
            st.empty()

        if self.diff.error:
            st.error(f"⚠️ Error: {self.diff.error}")
        else:
            st.empty()

        # Show controls: status approval, refresh, link
        self._show_chart_diff_controls()

        if "metadata" in self.diff.change_types:
            self._show_metadata_diff()

        # SHOW MODIFIED CHART
        if self.diff.is_modified:
            tab1, tab2, tab3 = st.tabs(["Charts", "Config diff", "Status log"])
            with tab1:
                self._show_chart_comparison()
            with tab2:
                self._show_config_diff()
            with tab3:
                self._show_approval_history()

        # SHOW NEW CHART
        elif self.diff.is_new:
            tab1, tab2 = st.tabs(["Chart", "Status log"])
            with tab1:
                self._show_chart_comparison()
            with tab2:
                self._show_approval_history()

    @st.fragment
    def show(self):
        """Show chart diff."""
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


def compare_dictionaries(dix_1: Dict[str, Any], dix_2: Dict[str, Any]):
    """Get diff of two dictionaries.

    Useful for chart config diffs, indicator metadata diffs, etc.
    """
    d1 = json.dumps(dix_1, indent=4)
    d2 = json.dumps(dix_2, indent=4)

    diff = difflib.unified_diff(
        d1.splitlines(keepends=True),
        d2.splitlines(keepends=True),
        fromfile="production",
        tofile="staging",
    )

    diff_string = "".join(diff)

    return diff_string


def st_show_diff(diff_str):
    """Display diff."""
    st.code(diff_str, line_numbers=True, language="diff")


def _show_dict_diff(dix_1: Dict[str, Any], dix_2: Dict[str, Any]):
    """Show diff of two dictionaries.

    Used to show chart config diffs, indicator metadata diffs, etc.
    """
    diff_str = compare_dictionaries(dix_1, dix_2)
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
