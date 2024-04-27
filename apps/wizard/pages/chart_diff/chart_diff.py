from typing import Optional

import streamlit as st

from etl import grapher_model as gm


class ChartDiffModified:
    def __init__(self, source_chart: gm.Chart, target_chart: Optional[gm.Chart], approval_status):
        self.source_chart = source_chart
        self.target_chart = target_chart
        self.approval_status = approval_status
        if target_chart:
            assert source_chart.id == target_chart.id, "Missmatch in chart ids between Target and Source!"
        self.chart_id = source_chart.id

    @property
    def approved(self):
        return self.approval_status == "approved"

    @property
    def is_new(self):
        return not self.is_modified

    @property
    def is_modified(self) -> bool:
        return self.target_chart is not None

    @classmethod
    def from_chart_id(cls, chart_id, source_session, target_session=None):
        """Get chart diff from chart id.

        - Get charts from source and target
        - Get its approval state
        - Build diff object
        """
        # Get charts
        source_chart = gm.Chart.load_chart(source_session, chart_id=chart_id)
        if target_session is not None:
            target_chart = gm.Chart.load_chart(target_session, chart_id=chart_id)
        else:
            target_chart = None
        # It can happen that both charts have the same ID, but are completely different (this
        # happens when two charts are created independently and have different slugs)
        if target_chart and source_chart.slug != target_chart.slug:
            target_chart = None

        # Get approval status
        approval_status = gm.ChartDiffApprovals.latest_chart_status(
            source_session,
            chart_id,
            source_chart.updatedAt,
            target_chart.updatedAt if target_chart else None,
        )

        # Build object
        chart_diff = cls(source_chart, target_chart, approval_status)

        return chart_diff

    def sync(self, source_session, target_session=None):
        """Sync chart diff."""

        # Synchronize with latest chart from source environment
        self = self.from_chart_id(
            chart_id=self.chart_id,
            source_session=source_session,
            target_session=target_session,
        )

    def update_state(self, session) -> None:
        """Update the state of the chart diff."""
        # Update status variable
        self.approval_status = "approved" if self.approval_status == "rejected" else "rejected"

        # Update approval status (in database)
        st.toast(f"Updating state for **chart {self.chart_id}** to `{self.approval_status}`")
        assert self.chart_id
        if self.is_modified:
            assert self.target_chart
        approval = gm.ChartDiffApprovals(
            chartId=self.chart_id,
            sourceUpdatedAt=self.source_chart.updatedAt,
            targetUpdatedAt=None if self.is_new else self.target_chart.updatedAt,  # type: ignore
            status="approved" if self.approved else "rejected",
        )

        session.add(approval)
        session.commit()
