import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from etl import grapher_model as gm
from etl.db import read_sql

ADMIN_GRAPHER_USER_ID = 1


class ChartDiff:
    # Chart in source environment
    source_chart: gm.Chart
    # Chart in target environment (if new in source environment, there won't be one)
    target_chart: Optional[gm.Chart]
    # Three state: 'approved', 'pending', 'rejected'
    approval_status: gm.CHART_DIFF_STATUS | str
    # DataFrame for all variables with columns dataChecksum and metadataChecksum
    # If True, then the checksum has changed
    modified_checksum: Optional[pd.DataFrame]

    def __init__(
        self,
        source_chart: gm.Chart,
        target_chart: Optional[gm.Chart],
        approval_status: gm.CHART_DIFF_STATUS | str,
        modified_checksum: Optional[pd.DataFrame] = None,
    ):
        self.source_chart = source_chart
        self.target_chart = target_chart
        self.approval_status = approval_status
        if target_chart:
            assert source_chart.id == target_chart.id, "Missmatch in chart ids between Target and Source!"
        self.chart_id = source_chart.id
        self.modified_checksum = modified_checksum

        # Cached
        self._in_conflict = None
        self._change_types = None

    @property
    def is_reviewed(self) -> bool:
        """Check if chart has been reviewed (approved or rejected)."""
        return self.is_approved or self.is_rejected

    @property
    def is_approved(self) -> bool:
        """Check if chart has been approved."""
        return self.approval_status == gm.ChartStatus.APPROVED.value

    @property
    def is_rejected(self) -> bool:
        """Check if the chart has been rejected."""
        return self.approval_status == gm.ChartStatus.REJECTED.value

    @property
    def is_pending(self) -> bool:
        """Check if the chart has status pending."""
        return self.approval_status == gm.ChartStatus.PENDING.value

    @property
    def is_modified(self) -> bool:
        """Check if the chart is a modification from an existing one in target."""
        return self.target_chart is not None

    @property
    def is_new(self) -> bool:
        """Check if the chart in source is new (compared to target)."""
        return not self.is_modified

    @property
    def is_draft(self) -> bool:
        """Check if the chart is a draft."""
        return self.source_chart.publishedAt is None

    @property
    def latest_update(self) -> dt.datetime:
        """Get latest time of change (either be staging or live)."""
        if self.target_chart is None:
            return self.source_chart.updatedAt
        else:
            return max([self.source_chart.updatedAt, self.target_chart.updatedAt])

    @property
    def slug(self) -> str:
        """Get slug of the chart.

        If slug of the chart miss-matches between target and source sessions, an error is displayed.
        """
        if self.target_chart:
            assert self.source_chart.config["slug"] == self.target_chart.config["slug"], "Slug mismatch!"
        return self.source_chart.config.get("slug", "no-slug")

    @property
    def in_conflict(self) -> bool:
        """Check if the chart in target is newer than the source."""
        if self._in_conflict is None:
            if self.target_chart is None:
                return False
            self._in_conflict = self.target_chart.updatedAt > self.source_chart.updatedAt
        return self._in_conflict

    @property
    def change_types(self) -> list[str]:
        """Return list of chart changes.

        This only applies if is_modified is True (i.e. is_new is False).

        Possible change types are:
            - data: changes in data
            - metadata: changes in metadata
            - config: changes in chart config

        If the chartdiff concerns a new chart, this returns an empty list.
        """
        if self._change_types is None:
            # Get change types
            self._change_types = []
            if not self.is_new:
                if self.modified_checksum is not None:
                    if self.modified_checksum["dataChecksum"].any():
                        self._change_types.append("data")
                    if self.modified_checksum["metadataChecksum"].any():
                        self._change_types.append("metadata")
                # if chart hasn't been edited by Admin, then disregard config change (it could come from having out of sync MySQL
                # against master)
                if (
                    self.target_chart
                    and not self.configs_are_equal()
                    and self.source_chart.lastEditedByUserId == ADMIN_GRAPHER_USER_ID
                ):
                    self._change_types.append("config")

                # TODO: Should uncomment this maybe?
                # assert self._change_types != [], "No changes detected!"

        return self._change_types

    @classmethod
    def from_chart_ids(cls, chart_ids, source_session: Session, target_session: Optional[Session] = None):
        """Get chart diffs from chart ids."""
        # Get charts from db and save them in memory as dictionaries: chart_id -> chart
        source_charts = gm.Chart.load_charts(source_session, chart_ids=chart_ids)
        source_charts = {chart.id: chart for chart in source_charts}

        if target_session is not None:
            try:
                target_charts = gm.Chart.load_charts(target_session, chart_ids=chart_ids)
            except NoResultFound:
                target_charts = {}
            else:
                target_charts = {chart.id: chart for chart in target_charts}
        else:
            target_charts = {}

        # Get approval status
        target_updated_ats = []
        for chart_id in chart_ids:
            if target_charts.get(chart_id) is not None:
                target_updated_ats.append(target_charts[chart_id].updatedAt)
            else:
                target_updated_ats.append(None)
        approval_statuses = gm.ChartDiffApprovals.latest_chart_status_batch(
            source_session,
            chart_ids,
            [source_charts[chart_id].updatedAt for chart_id in chart_ids],
            target_updated_ats,
        )
        approval_statuses = dict(zip(chart_ids, approval_statuses))

        # Get checksums
        checksums_source = gm.Chart.load_variables_checksums(source_session, chart_ids)
        checksums_Target = gm.Chart.load_variables_checksums(target_session, chart_ids)

        # Build chart diffs
        for chart_id, source_chart in source_charts.items():
            # Get target chart (if exists)
            target_chart = target_charts.get(chart_id)
            if target_chart and source_chart.createdAt != target_chart.createdAt:
                target_chart = None
            ## Checks
            if target_chart:
                assert source_chart.createdAt == target_chart.createdAt, "CreatedAt mismatch!"

            # Approval status
            assert chart_id in approval_statuses, f"Approval status not found for chart {chart_id}"
            approval_status = approval_statuses[chart_id]

            # Checksums

            # Build chart diff object
            chart_diff = cls(source_chart, target_chart, approval_status, modified_checksum)

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

        # Load checksums for underlying indicators
        # TODO: is this fast enough for large datasets? maybe we should avoid dataframes at all
        if target_chart and target_session is not None:
            source_df = source_chart.load_variable_checksums(source_session)
            target_df = target_chart.load_variable_checksums(target_session)
            source_df, target_df = source_df.align(target_df)
            modified_checksum = source_df != target_df

            # If checksum has not been filled yet, assume unchanged
            modified_checksum[target_df.isna()] = False
        else:
            modified_checksum = None

        # Build object
        chart_diff = cls(source_chart, target_chart, approval_status, modified_checksum)

        return chart_diff

    def get_all_approvals(self, session: Session) -> List[gm.ChartDiffApprovals]:
        """Get history of chart diff."""
        # Get history
        history = gm.ChartDiffApprovals.get_all(
            session,
            chart_id=self.chart_id,
        )
        return history

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
        self.set_status(session, gm.ChartStatus.APPROVED.value)

    def reject(self, session: Session) -> None:
        """Reject chart diff."""
        # Update status variable
        self.set_status(session, gm.ChartStatus.REJECTED.value)

    def unreview(self, session: Session) -> None:
        """Set chart diff to pending."""
        # Update status variable
        self.set_status(session, gm.ChartStatus.PENDING.value)

    def set_status(self, session: Session, status: gm.CHART_DIFF_STATUS | str) -> None:
        """Update the state of the chart diff."""
        # Only perform action if status changes!
        if self.approval_status != status:
            # Update status variable
            self.approval_status = status

            # Update approval status (in database)
            assert self.chart_id
            if self.is_modified:
                assert self.target_chart
            approval = gm.ChartDiffApprovals(
                chartId=self.chart_id,
                sourceUpdatedAt=self.source_chart.updatedAt,
                targetUpdatedAt=None if self.is_new else self.target_chart.updatedAt,  # type: ignore
                status=self.approval_status,  # type: ignore
            )
            session.add(approval)
            session.commit()

    def configs_are_equal(self) -> bool:
        """Compare two chart configs, ignoring version, id and isPublished."""
        assert self.target_chart is not None, "Target chart is None!"
        exclude_keys = ("id", "isPublished", "bakedGrapherURL", "adminBaseUrl", "dataApiUrl")
        config_1 = {k: v for k, v in self.source_chart.config.items() if k not in exclude_keys}
        config_2 = {k: v for k, v in self.target_chart.config.items() if k not in exclude_keys}
        return config_1 == config_2

    @property
    def details(self):
        return {
            "chart_id": self.chart_id,
            "is_approved": self.is_approved,
            "is_pending": self.is_pending,
            "is_rejected": self.is_rejected,
            "is_reviewed": self.is_reviewed,
            "is_new": self.is_new,
        }


class ChartDiffsLoader:
    """Detect charts that differ between staging and production."""

    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.df = self.load_df()

        # Cache
        self._diffs: Dict[str, Any] | None = None

    def load_df(self) -> pd.DataFrame:
        """Load changes in charts between environments from sessions."""
        with Session(self.source_engine) as source_session:
            with Session(self.target_engine) as target_session:
                return modified_charts_by_admin(source_session, target_session)

    @property
    def chart_ids_all(self):
        return set(self.df.index[self.df.any(axis=1)])

    def get_chart_ids(
        self,
        config: bool | None = None,
        data: bool | None = None,
        metadata: bool | None = None,
    ):
        """Get chart ids based on changes in config, data or metadata."""
        return set(
            self.df.index[
                (self.df.configEdited & config) | (self.df.dataEdited & data) | (self.df.metadataEdited & metadata)
            ]
        )

    def get_diffs_2(
        self,
        config: bool = True,
        data: bool = False,
        metadata: bool = False,
        max_workers: int = 10,
        sync: bool = False,
    ):
        if sync:
            self.df = self.load_df()

        # Get ids of charts with relevant changes
        chart_ids = self.get_chart_ids(config, data, metadata)

    def get_diffs(
        self,
        config: bool = True,
        data: bool = False,
        metadata: bool = False,
        max_workers: int = 10,
        sync: bool = False,
    ):
        """Get chart diffs from Grapher.

        This means, checking for chart changes in the database.

        Changes in charts can be due to: chart config changes, changes in indicator timeseries, in indicator metadata, etc.
        """
        if sync:
            self.df = self.load_df()

        # Get ids of charts with relevant changes
        chart_ids = self.get_chart_ids(config, data, metadata)
        # chart_ids = self.chart_ids_all

        # Get all chart diffs in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            chart_diffs_futures = {
                chart_id: executor.submit(
                    _get_chart_diff_from_grapher,
                    chart_id,
                    self.source_engine,
                    self.target_engine,
                )
                for chart_id in chart_ids
            }
            chart_diffs = {}
            for chart_id, future in chart_diffs_futures.items():
                chart_diffs[chart_id] = future.result()

        self._diffs = chart_diffs
        return chart_diffs

    def get_diffs_summary_df(self, cache: bool = False, **kwargs) -> pd.DataFrame:
        """Get dataframe with summary of current chart diffs.

        Details include whether the chart is new, approved, pending or rejected, etc.

        cache: Use cache (latest value retrieved by get_diffs if exists.
        """
        if cache and self._diffs is not None:
            chart_diffs = self._diffs
        else:
            chart_diffs = self.get_diffs(**kwargs)

        summary = []
        for diff in chart_diffs.values():
            summary.append(diff.details)
        return pd.DataFrame(summary)


def modified_charts_by_admin(source_session: Session, target_session: Session) -> pd.DataFrame:
    """Get charts that have been modified in staging.

    - It includes charts with different config, data or metadata checksums.
    - It assumes that all changes on staging server are done by Admin user with ID = 1. That is, if there are changes by a different user in staging, they are not included.

    The returned object is a dataframe with the following columns:
        - chartId (index): ID of the chart
        - dataEdited: True if data checksum has changed
        - metadataEdited: True if metadata checksum has changed
        - configEdited: True if config has changed

        TODO:
        - newInSource: True if the chart is new in source environment.
        - newInTarget: True if the chart is new in target environment.
        - deletedInSource: True if the chart is deleted in source environment.
        - deletedInTarget: True if the chart is deleted in target environment.
    """
    # Get timestamp of creation of databas (i.e. proxy for staging server creation)
    query_ts = "show table status like 'charts'"
    df = read_sql(query_ts, source_session)
    assert len(df) == 1
    TIMESTAMP_STAGING_CREATION = df["Create_time"].item()

    # TODO: we could aggregate it by charts and get a combined checksum, for now we want
    #   a view with variable granularity
    # get modified charts and charts from modified datasets
    base_q = """
    select
        v.id as variableId,
        cd.chartId,
        v.dataChecksum,
        v.metadataChecksum,
        MD5(c.config) as chartChecksum,
        c.lastEditedByUserId as chartLastEditedByUserId,
        c.publishedByUserId as chartPublishedByUserId,
        c.lastEditedAt as chartLastEditedAt,
        d.dataEditedByUserId,
        d.metadataEditedByUserId
    from chart_dimensions as cd
    join charts as c on cd.chartId = c.id
    join variables as v on cd.variableId = v.id
    join datasets as d on v.datasetId = d.id
    where
    """
    # NOTE: We assume that all changes on staging server are done by Admin user with ID = 1. This is
    #   set automatically if you use STAGING env variable.
    where = """
        -- only compare datasets or charts that have been updated on staging server
        -- by Admin user
        (
            (c.lastEditedByUserId = 1 or c.publishedByUserId = 1)
            or
            -- include all charts from datasets that have been updated
            (d.dataEditedByUserId = 1 or d.metadataEditedByUserId = 1)
        )
    """
    source_df = read_sql(base_q + where, source_session)

    # no charts, return empty dataframe
    if source_df.empty:
        return pd.DataFrame(columns=["chartId", "dataEdited", "metadataEdited", "configEdited"]).set_index("chartId")

    # read those charts from target
    where = """
        c.id in %(chart_ids)s
    """
    target_df = read_sql(base_q + where, target_session, params={"chart_ids": tuple(source_df.chartId.unique())})

    source_df = source_df.set_index(["chartId", "variableId"])
    target_df = target_df.set_index(["chartId", "variableId"])

    # charts not edited by Admin and with null checksums should be excluded
    ix = (
        (source_df.chartLastEditedByUserId != 1)
        & (source_df.chartPublishedByUserId != 1)
        & (source_df.dataChecksum.isnull() | source_df.metadataChecksum.isnull())
    )
    source_df = source_df[~ix]

    # align dataframes with left join (so that source has non-null values)
    # NOTE: new charts will be already in source
    # NOTE: chart IDs and variable IDs in both environments do not necessarily correspond to the same charts or indicators! While there might be a chart with ID X in both environments that corresponds to different charts, it is rare that there is a chart with ID X using indicator with ID Y, that are different. However, this can't be ruled out. Therefore, aligning source_df and target_df by chartId and variableId can fail sometimes!
    source_df, target_df = source_df.align(target_df, join="left")

    # return differences in data / metadata / config
    diff = (
        (source_df != target_df)
        .groupby("chartId")
        .max()
        .rename(
            columns={
                "dataChecksum": "dataEdited",
                "metadataChecksum": "metadataEdited",
                "chartChecksum": "configEdited",
            }
        )
    )
    diff = diff[["dataEdited", "metadataEdited", "configEdited"]]

    # Add flag 'edited in staging'
    edited = source_df.groupby("chartId")[["chartLastEditedAt"]].max()
    edited["editedInStaging"] = edited >= TIMESTAMP_STAGING_CREATION
    diff = diff.merge(edited[["editedInStaging"]], left_index=True, right_index=True, how="left")
    assert diff.editedInStaging.notna().all(), "editedInStaging has missing values! This might be due to `diff` and `eidted` dataframes not having the same number of rows."

    # If chart hasn't been edited by Admin, then make `configEdited` false
    # This can happen when you merge master to your branch and staging rebuilds a dataset.
    # Then dataset will be edited by Admin and will be included, but your charts might be outdated
    # compared to production. Hence, only consider config updates for charts edited by Admin.
    chart_ids = source_df[
        (source_df.chartLastEditedByUserId != 1) & (source_df.chartPublishedByUserId != 1)
    ].index.get_level_values("chartId")
    diff.loc[chart_ids, "configEdited"] = False

    # Remove charts with no changes
    diff = diff[diff.any(axis=1)]

    return diff


def get_chart_diffs_from_grapher(
    source_engine: Engine, target_engine: Engine, max_workers: int = 10
) -> dict[int, ChartDiff]:
    """Get chart diffs from Grapher.

    This means, checking for chart changes in the database.

    Changes in charts can be due to: chart config changes, changes in indicator timeseries, in indicator metadata, etc.
    """
    chart_diffs = ChartDiffsLoader(
        source_engine,
        target_engine,
    ).get_diffs(
        config=True,
        metadata=True,
        data=True,
        max_workers=max_workers,
    )

    return chart_diffs


def _get_chart_diff_from_grapher(chart_id: int, source_engine: Engine, target_engine: Engine) -> ChartDiff:
    with Session(source_engine) as source_session, Session(target_engine) as target_session:
        return ChartDiff.from_chart_id(
            chart_id=chart_id,
            source_session=source_session,
            target_session=target_session,
        )
