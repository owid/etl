from datetime import datetime
from typing import List, Optional

import streamlit as st
from sqlalchemy.orm import Session

import etl.grapher_model as gm
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.app_pages.chart_diff.config_diff import st_show_diff
from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
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


class ChartDiffShow:
    """Handle a chart-diff and show it.

    Showing a chart-diff involves showing various parts: the visualisation of the chart, the diff of the chart config, the history of approvals, and various controls.
    """

    def __init__(
        self,
        diff: ChartDiffModified,
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
        label += f":break[{' '.join(tags)}]"
        return label

    @property
    def kwargs_diff(self):
        """TODO: describe function"""
        # Get the right arguments for the toggle, button and diff show
        if self.diff.is_modified:
            # Arguments for the toggle
            # label_tgl = "Approved new chart version"

            # Arguments for diff
            return {
                "source_chart": self.diff.source_chart,
                "target_chart": self.diff.target_chart,
            }
        elif self.diff.is_new:
            # Arguments for the toggle
            # label_tgl = "Approved new chart"

            # Arguments for diff
            return {
                "source_chart": self.diff.source_chart,
            }
        else:
            raise ValueError("chart_diff show have flags `is_modified = not is_new`.")

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
        diff_new = ChartDiffModified.from_chart_id(
            chart_id=self.diff.chart_id,
            source_session=self.source_session,
            target_session=self.target_session,
        )
        st.session_state.chart_diffs[self.diff.chart_id] = diff_new

    def show_approval_history(self):
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

    @property
    def _header_production_chart(self):
        """Header for the production chart."""
        # Everything is fine
        if not self.diff.target_is_newer:
            text_production = f"Production ┃ _{pretty_date(self.diff.target_chart)}_"
        # Conflict with live
        else:
            text_production = f":red[Production ┃ _{pretty_date(self.diff.target_chart)}_] ⚠️"

        return text_production

    @property
    def _header_staging_chart(self):
        """Header for staging chart."""
        # Everything is fine
        if not self.diff.target_is_newer:
            text_staging = f":green[New version ┃ _{pretty_date(self.diff.source_chart)}_]"
        # Conflict with live
        else:
            text_staging = f"New version ┃ _{pretty_date(self.diff.source_chart)}_"

        return text_staging

    def _show_chart_comparison(
        self,
        arrange_vertical=False,
    ) -> None:
        """Show charts (horizontally or vertically)."""
        # Only one chart: new chart
        if self.diff.target_chart is None:
            st.markdown(f"New version ┃ _{pretty_date(self.diff.source_chart)}_")
            chart_html(self.diff.source_chart.config, owid_env=SOURCE)
        # Two charts, actual diff
        else:
            # Show charts
            if arrange_vertical:
                self._show_charts_comparison_v(
                    self.diff.target_chart,
                    self.diff.source_chart,
                    self._header_production_chart,
                    self._header_staging_chart,
                )
            else:
                self._show_charts_comparison_h(
                    self.diff.target_chart,
                    self.diff.source_chart,
                    self._header_production_chart,
                    self._header_staging_chart,
                )

    def _show_charts_comparison_v(self, target_chart, source_chart, text_production, text_staging):
        """Show charts on top of each other."""
        # Chart production
        if self.diff.target_is_newer:
            help_text = _get_chart_text_help_production()
            st.markdown(text_production, help=help_text)
        else:
            st.markdown(text_production)
        chart_html(target_chart.config, owid_env=TARGET)

        # Chart staging
        st.markdown(text_staging)
        chart_html(source_chart.config, owid_env=SOURCE)

    def _show_charts_comparison_h(self, target_chart, source_chart, text_production, text_staging):
        """Show charts next to each other."""
        # Create two columns for the iframes
        col1, col2 = st.columns(2)

        with col1:
            if self.diff.target_is_newer:
                help_text = _get_chart_text_help_production()
                st.markdown(text_production, help=help_text)
            else:
                st.markdown(text_production)
            chart_html(target_chart.config, owid_env=TARGET)
        with col2:
            st.markdown(text_staging)
            chart_html(source_chart.config, owid_env=SOURCE)

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

    def _show(self) -> None:
        """Show chart diff."""
        # Show controls: status approval, refresh, link
        self._show_chart_diff_controls()

        # SHOW MODIFIED CHART
        if self.diff.is_modified:
            # CONFLICT RESOLVER
            if self.diff.target_is_newer:
                tab1, tab2, tab2b, tab3 = st.tabs(["Charts", "Config diff", "⚠️ Conflict resolver", "Change history"])
                with tab2b:
                    st.warning(
                        "This is under development! For now, please resolve the conflict manually by integrating the changes in production into the chart in staging server."
                    )
                    config_compare = compare_chart_configs(
                        self.diff.target_chart.config,  # type: ignore
                        self.diff.source_chart.config,
                    )

                    if config_compare:
                        with st.form("conflict-resolve-form"):
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
                                    key=f"conflict-{field['key']}",
                                    # horizontal=True,
                                )
                                st.text_input(
                                    "Custom value",
                                    label_visibility="collapsed",
                                    placeholder="Enter a custom value",
                                    key=f"conflict-custom-{field['key']}",
                                )
                            st.form_submit_button("Resolve", help="This will update the chart in the staging server.")
            else:
                tab1, tab2, tab3 = st.tabs(["Charts", "Config diff", "Change history"])
            with tab1:
                arrange_vertical = st.session_state.get(
                    f"arrange-charts-vertically-{self.diff.chart_id}", False
                ) | st.session_state.get("arrange-charts-vertically", False)
                # Chart diff
                self._show_chart_comparison(arrange_vertical=arrange_vertical)
                st.toggle(
                    "Arrange charts vertically",
                    key=f"arrange-charts-vertically-{self.diff.chart_id}",
                    # on_change=None,
                )
            with tab2:
                assert self.diff.target_chart is not None
                st_show_diff(self.diff.target_chart.config, self.diff.source_chart.config)
            with tab3:
                self.show_approval_history()

        # SHOW NEW CHART
        elif self.diff.is_new:
            tab1, tab2 = st.tabs(["Chart", "Change history"])
            with tab1:
                self._show_chart_comparison()
            with tab2:
                self.show_approval_history()

    def show(self):
        """Show chart diff."""
        # Show in expander or not
        if self.expander:
            with st.expander(self.box_label, not self.diff.is_reviewed):
                self._show()
        else:
            self._show()


def st_show(
    diff: ChartDiffModified,
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


def compare_chart_configs(c1, c2):
    keys = set(c1.keys()).union(c2.keys())
    diff_list = []

    KEYS_IGNORE = {
        "bakedGrapherURL",
        "adminBaseUrl",
        "dataApiUrl",
        "version",
    }
    for key in keys:
        if key in KEYS_IGNORE:
            continue
        value1 = c1.get(key)
        value2 = c2.get(key)
        if value1 != value2:
            diff_list.append({"key": key, "value1": value1, "value2": value2})

    return diff_list


def _get_chart_header_production_chart(prod_is_newer: bool, production_chart):
    # Everything is fine
    if not prod_is_newer:
        text_production = f"Production ┃ _{pretty_date(production_chart)}_"
    # Conflict with live
    else:
        text_production = f":red[Production ┃ _{pretty_date(production_chart)}_] ⚠️"

    return text_production


def _get_chart_text_help_production():
    return "The chart in production was modified after creating the staging server. Please resolve the conflict by integrating the latest changes from production into staging."


def pretty_date(chart):
    """Obtain prettified date from a chart.

    Format is:
        - Previous years: `Jan 10, 2020 10:15`
        - This year: `Mar 15, 10:15` (no need to explicitly show the year)
    """
    if chart.updatedAt.year == datetime.now().date().year:
        return chart.updatedAt.strftime("%b %d, %H:%M")
    else:
        return chart.updatedAt.strftime("%b %d, %Y %H:%M")
