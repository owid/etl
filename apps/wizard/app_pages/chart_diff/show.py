"""This module contains a main class `ChartDiffShow`, which handles all the visualisation aspect of chart diffs.


If you want to learn more about it, start from its `show` method.
"""
import difflib
import json
from typing import List, Optional

import streamlit as st
from sqlalchemy.orm import Session

import etl.grapher_model as gm
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff
from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET, compare_chart_configs, prettify_date
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
        for change in self.diff.checksum_changes():
            tags.append(f":red-background[**{change}**]")
        label += f":break[{' '.join(tags)}]"
        return label

    @property
    def status_names(self) -> List[str]:
        """List with names of accepted statuses."""
        return list(DISPLAY_STATE_OPTIONS.keys())

    def _push_status(self, session: Optional[Session] = None) -> None:
        """Change state of the ChartDiff based on session state."""
        if session is None:
            session = self.source_session
        with st.spinner():
            status = st.session_state[f"radio-{self.diff.chart_id}"]
            self.diff.set_status(session=session, status=status)

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

    def _show_chart_diff_controls(self):
        # Three columns: status, refresh, link
        col1, col2, col3 = st.columns(3)

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
                st.caption(f"**{OWID_ENV.wizard_url}?page=chart-diff&chart_id={self.diff.chart_id}**")

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

        config_1 = json.dumps(config_1, indent=4)
        config_2 = json.dumps(config_2, indent=4)

        diff = difflib.unified_diff(
            config_1.splitlines(keepends=True),
            config_2.splitlines(keepends=True),
            fromfile="production",
            tofile="staging",
        )

        diff_string = "".join(diff)

        st.code(diff_string, line_numbers=True, language="diff")

    def _show_conflict_resolver(self) -> None:
        """Resolve conflicts between charts in target and source.

        Sometimes, someone might edit a chart in production while we work on it on staging.
        """
        st.warning(
            "This is under development! For now, please resolve the conflict manually by integrating the changes in production into the chart in staging server."
        )
        config_compare = compare_chart_configs(
            self.diff.target_chart.config,  # type: ignore
            self.diff.source_chart.config,
        )

        if config_compare:
            with st.form(f"conflict-form-{self.diff.chart_id}"):
                st.markdown("### Conflict resolver")
                st.markdown(
                    "Find below the chart config fields that do not match. Choose the value you want to keep for each of the fields (or introduce a new one)."
                )
                for field in config_compare:
                    st.radio(
                        f"**{field['key']}**",
                        options=[field["value1"], field["value2"]],
                        format_func=lambda x: f"{field['value1']} `PROD`"
                        if x == field["value1"]
                        else f"{field['value2']} `staging`",
                        key=f"conflict-radio-{self.diff.chart_id}-{field['key']}",
                        # horizontal=True,
                    )
                    st.text_input(
                        "Custom value",
                        label_visibility="collapsed",
                        placeholder="Enter a custom value",
                        key=f"conflict-custom-{self.diff.chart_id}-{field['key']}",
                    )
                st.form_submit_button("Resolve", help="This will update the chart in the staging server.")

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
        # Show controls: status approval, refresh, link
        self._show_chart_diff_controls()

        # SHOW MODIFIED CHART
        if self.diff.is_modified:
            if not self.diff.in_conflict:
                tab1, tab2, tab3 = st.tabs(["Charts", "Config diff", "Change history"])
            else:
                # Resolve conflict
                tab1, tab2, tab2b, tab3 = st.tabs(["Charts", "Config diff", "⚠️ Conflict resolver", "Change history"])
                with tab2b:
                    self._show_conflict_resolver()
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
