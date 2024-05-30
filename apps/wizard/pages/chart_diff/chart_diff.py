import datetime as dt
from typing import Optional, get_args

import streamlit as st
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from etl import grapher_model as gm


class ChartDiffModified:
    source_chart: gm.Chart
    target_chart: Optional[gm.Chart]
    approval_status: gm.CHART_DIFF_STATUS

    def __init__(self, source_chart: gm.Chart, target_chart: Optional[gm.Chart], approval_status: gm.CHART_DIFF_STATUS):
        self.source_chart = source_chart
        self.target_chart = target_chart
        self.approval_status = approval_status
        if target_chart:
            assert source_chart.id == target_chart.id, "Missmatch in chart ids between Target and Source!"
        self.chart_id = source_chart.id

    @property
    def approved(self) -> bool:
        return self.approval_status == "approved"

    @property
    def unapproved(self) -> bool:
        return self.approval_status == "unapproved"

    @property
    def is_new(self):
        return not self.is_modified

    @property
    def latest_update(self) -> dt.datetime:
        """Get latest time of change (either be staging or live)."""
        if self.target_chart is None:
            return self.source_chart.updatedAt
        else:
            return max([self.source_chart.updatedAt, self.target_chart.updatedAt])

    @property
    def is_modified(self) -> bool:
        return self.target_chart is not None

    @classmethod
    def from_chart_id(cls, chart_id, source_session: Session, target_session: Optional[Session] = None):
        """Get chart diff from chart id.

        - Get charts from source and target
        - Get its approval state
        - Build diff object
        """
        # Get charts
        source_chart = gm.Chart.load_chart(source_session, chart_id=chart_id)
        if target_session is not None:
            try:
                target_chart = gm.Chart.load_chart(target_session, chart_id=chart_id)
            except NoResultFound:
                target_chart = None
        else:
            target_chart = None

        # It can happen that both charts have the same ID, but are completely different (this
        # happens when two charts are created independently on two servers). If they
        # have same createdAt then they are the same chart.
        if target_chart and source_chart.createdAt != target_chart.createdAt:
            target_chart = None

        # Checks
        if target_chart:
            assert source_chart.createdAt == target_chart.createdAt, "CreatedAt mismatch!"

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

    def sync(self, source_session: Session, target_session: Optional[Session] = None):
        """Sync chart diff."""

        # Synchronize with latest chart from source environment
        self = self.from_chart_id(
            chart_id=self.chart_id,
            source_session=source_session,
            target_session=target_session,
        )

    def approve(self, session: Session) -> None:
        """Approve chart diff."""
        # Update status variable
        self.set_status(session, "approved")

    def unapprove(self, session: Session) -> None:
        """Unapprove chart diff."""
        # Update status variable
        self.set_status(session, "unapproved")

    def switch_state(self, session: Session) -> None:
        """Switch the state of the chart diff. This will work only with two states: approved and unapproved."""
        # Update status variable
        assert get_args(gm.CHART_DIFF_STATUS) == ("approved", "unapproved")
        status = "approved" if self.unapproved else "unapproved"
        self.set_status(session, status)

    def set_status(self, session: Session, status: gm.CHART_DIFF_STATUS) -> None:
        """Update the state of the chart diff."""
        # Only perform action if status changes!
        if self.approval_status != status:
            # Update status variable
            self.approval_status = status

            # Update approval status (in database)
            st.toast(f"Updating state for **chart {self.chart_id}** to `{self.approval_status}`")
            assert self.chart_id
            if self.is_modified:
                assert self.target_chart
            approval = gm.ChartDiffApprovals(
                chartId=self.chart_id,
                sourceUpdatedAt=self.source_chart.updatedAt,
                targetUpdatedAt=None if self.is_new else self.target_chart.updatedAt,  # type: ignore
                status="approved" if self.approved else "unapproved",
            )

            session.add(approval)
            session.commit()

    def configs_are_equal(self) -> bool:
        """Compare two chart configs, ignoring version, id and isPublished."""
        assert self.target_chart is not None, "Target chart is None!"
        exclude_keys = ("version", "id", "isPublished")
        config_1 = {k: v for k, v in self.source_chart.config.items() if k not in exclude_keys}
        config_2 = {k: v for k, v in self.target_chart.config.items() if k not in exclude_keys}
        return config_1 == config_2
