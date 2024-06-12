"""This module contains a main class `ChartDiffShow`, which handles all the visualisation aspect of chart diffs.


If you want to learn more about it, start from its `show` method.
"""
import difflib
import json
from typing import Any, Dict, List, Optional

import streamlit as st
from sqlalchemy.orm import Session

import etl.grapher_model as gm
from apps.backport.datasync.data_metadata import variable_metadata_df_from_s3
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff
from apps.wizard.app_pages.chart_diff.conflict_resolver import st_show_conflict_resolver
from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET, prettify_date
from apps.wizard.utils import chart_html
from apps.wizard.utils.env import OWID_ENV

# How to display the various chart review statuses
DISPLAY_STATE_OPTIONS = {
    gm.ChartStatus.APPROVED.value: {
        "label": "Approve",
        "color": "green",
        "icon": "✅",
    },
    gm.ChartStatus.REJECTED.value: {
        "label": "Reject",
        "color": "red",
        "icon": "❌",
    },
    gm.ChartStatus.PENDING.value: {
        "label": "Pending",
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
        target_session: Optional[Session] = None,
        expander: bool = True,
        show_link: bool = True,
    ):
        self.diff = diff
        self.source_session = source_session
        self.target_session = target_session
        self.expander = expander
        self.show_link = show_link
        self._checksum_changes: List[str] | None = None

    @property
    def box_label(self):
        """Label of the expander box.

        This contains the state of the approval (by means of an emoji), the slug of the chart, and any tags (like "NEW" or "DRAFT").
        """
        emoji = DISPLAY_STATE_OPTIONS[self.diff.approval_status]["icon"]  # type: ignore
        label = f"{emoji} {self.diff.slug}"
        tags = []
        if self.diff.is_new:
            tags.append(" :blue-background[**NEW**]")
        if self.diff.is_draft:
            tags.append(" :gray-background[**DRAFT**]")
        for change in self.checksum_changes:
            tags.append(f":red-background[**{change.upper()} CHANGE**]")
        label += f":break[{' '.join(tags)}]"
        return label

    @property
    def status_names(self) -> List[str]:
        """List with names of accepted statuses."""
        return list(DISPLAY_STATE_OPTIONS.keys())

    @property
    def checksum_changes(self) -> List[str]:
        """List with names of checksum changes."""
        if self._checksum_changes is None:
            self._checksum_changes = self.diff.checksum_changes()
        return self._checksum_changes

    def clean_cache(self) -> None:
        """Clean temporary cached variables."""
        self._checksum_changes = None

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

    def _pull_latest_chart(self):
        """Get latest chart version from database."""
        diff_new = ChartDiff.from_chart_id(
            chart_id=self.diff.chart_id,
            source_session=self.source_session,
            target_session=self.target_session,
        )
        st.session_state.chart_diffs[self.diff.chart_id] = diff_new

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
        if st.button(
            "⚠️ Resolve conflict",
            key=f"resolve-conflict-{self.diff.slug}",
            help="This will update the chart in the staging server.",
            type="primary",
        ):
            self._show_conflict_resolver_modal()

    @st.experimental_dialog("Resolve conflict", width="large")  # type: ignore
    def _show_conflict_resolver_modal(self) -> None:
        """Show conflict resolver in modal page."""
        st_show_conflict_resolver(self.diff)

    def _show_chart_diff_controls(self):
        # Three columns: status, refresh, link
        col1, col2, col3 = st.columns([2, 1, 3])

        # Status of chart diff: approve, pending, reject
        with col1:
            st.radio(
                label="Approve or reject chart",
                key=f"radio-{self.diff.chart_id}",
                options=self.status_names,
                horizontal=True,
                format_func=lambda x: f":{DISPLAY_STATE_OPTIONS[x]['color']}-background[{DISPLAY_STATE_OPTIONS[x]['label']}]",
                index=self.status_names.index(self.diff.approval_status),  # type: ignore
                on_change=self._push_status,
            )

        # Refresh chart
        with col2:
            st.button(
                "🔄 Refresh",
                key=f"refresh-btn-{self.diff.chart_id}",
                on_click=self._pull_latest_chart,
                help="Get the latest version of the chart from the staging server.",
            )
        # Copy link
        if self.show_link:
            with col3:
                query_params = f"page=chart-diff&chart_id={self.diff.chart_id}"
                # st.caption(f"**{OWID_ENV.wizard_url}?{query_params}**")
                if OWID_ENV.wizard_url != OWID_ENV.wizard_url_remote:
                    st.caption(
                        f"**{OWID_ENV.wizard_url_remote}?{query_params}**",
                        help=f"Shown is the link to the remote chart-diff.\n\n Alternatively, local link: {OWID_ENV.wizard_url}?{query_params}",
                    )
                else:
                    st.caption(f"**{OWID_ENV.wizard_url}?{query_params}**")

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

    @st.experimental_dialog("Metadata differences", width="large")  # type: ignore
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
        if set(source_ids) != set(target_ids):
            st.warning(
                f"List of indicators in source and target differs. Can't render this section.\n\nSOURCE: {source_ids}\n\nTARGET: {target_ids}"
            )
        elif source_ids:
            with st.spinner("Getting metadata from S3..."):
                # Get metadata from source & target
                metadata_source = variable_metadata_df_from_s3(source_ids, env=SOURCE)
                metadata_target = variable_metadata_df_from_s3(source_ids, env=TARGET)

            for source, target, indicator_id in zip(metadata_source, metadata_target, source_ids):
                st.markdown(f"**Indicator ID: {indicator_id}**")
                if "catalogPath" in source and source["catalogPath"] != "":
                    st.caption(source["catalogPath"])
                _show_dict_diff(source, target)  # type: ignore

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
            chart_html(self.diff.target_chart.config, owid_env=TARGET)

            # Chart staging
            st.markdown(self._header_staging_chart)
            chart_html(self.diff.source_chart.config, owid_env=SOURCE)

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
                chart_html(self.diff.target_chart.config, owid_env=TARGET)
            with col2:
                st.markdown(self._header_staging_chart)
                chart_html(self.diff.source_chart.config, owid_env=SOURCE)

        # Only one chart: new chart
        if self.diff.target_chart is None:
            st.markdown(f"New version ┃ _{prettify_date(self.diff.source_chart)}_")
            chart_html(self.diff.source_chart.config, owid_env=SOURCE)
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
        # Ask user to resolve conflicts
        if self.diff.in_conflict:
            self._show_conflict_resolver()

        # Show controls: status approval, refresh, link
        self._show_chart_diff_controls()

        if "metadata" in self.checksum_changes:
            self._show_metadata_diff()

        # SHOW MODIFIED CHART
        if self.diff.is_modified:
            tab1, tab2, tab3 = st.tabs(["Charts", "Config diff", "Change history"])
            with tab1:
                self._show_chart_comparison()
            with tab2:
                self._show_config_diff()
            with tab3:
                self._show_approval_history()

        # SHOW NEW CHART
        elif self.diff.is_new:
            tab1, tab2 = st.tabs(["Chart", "Change history"])
            with tab1:
                self._show_chart_comparison()
            with tab2:
                self._show_approval_history()

    def show(self):
        """Show chart diff."""
        # Show in expander or not
        if self.expander:
            with st.expander(self.box_label, not self.diff.is_reviewed):
                self._show()
        else:
            self._show()

        self.clean_cache()


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
    target_session: Optional[Session] = None,
    expander: bool = True,
    show_link: bool = True,
) -> None:
    """Show the chart diff in Streamlit."""
    ChartDiffShow(
        diff=diff,
        source_session=source_session,
        target_session=target_session,
        expander=expander,
        show_link=show_link,
    ).show()
