import datetime as dt
import difflib
import pprint
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.utils import get_staging_creation_time
from apps.wizard.utils.io import get_all_changed_catalog_paths
from etl import grapher_model as gm
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.git_helpers import get_changed_files

log = get_logger()


# Some datasets like COVID or certain AI datasets use {TODAY} in their metadata, making the metadata dependent
# on the creation date. Merging a day later results in many metadata changes. The current workaround is to
# exclude these datasets from comparison, similar to what we do for data-diff.
EXCLUDE_METADATA_CHANGES = [
    "grapher/covid/latest",
    "grapher/climate/.*/surface_temperature_annual_average",
    "grapher/artificial_intelligence/.*/epoch",
]


class ChartDiff:
    # Chart in source environment
    source_chart: gm.Chart
    # Chart in target environment (if new in source environment, there won't be one)
    target_chart: Optional[gm.Chart]
    # DataFrame for all variables with columns dataChecksum and metadataChecksum
    # If True, then the checksum has changed
    modified_checksum: Optional[pd.DataFrame]
    # Whether the chart has been edited in staging
    edited_in_staging: Optional[bool]
    # Error preventing the chart from being synced
    error: Optional[str]

    def __init__(
        self,
        source_chart: gm.Chart,
        target_chart: Optional[gm.Chart],
        approval: gm.ChartDiffApprovals | None,
        conflict: gm.ChartDiffConflicts | None,
        # approval_status: gm.CHART_DIFF_STATUS | str,
        modified_checksum: Optional[pd.DataFrame] = None,
        edited_in_staging: Optional[bool] = None,
        error: Optional[str] = None,
    ):
        self.source_chart = source_chart
        self.target_chart = target_chart
        self.approval = approval
        self.conflict = conflict
        self.error = error
        if target_chart:
            assert source_chart.id == target_chart.id, "Missmatch in chart ids between Target and Source!"
        self.chart_id = source_chart.id
        self.modified_checksum = modified_checksum
        self.edited_in_staging = edited_in_staging

        # Cached
        self._in_conflict = None
        self._change_types = None
        self._approval_status: Optional[gm.CHART_DIFF_STATUS | str] = None

    def _clean_cache(self):
        self._in_conflict = None
        self._change_types = None
        self._approval_status = None

    @property
    def approval_status(self) -> gm.CHART_DIFF_STATUS | str:
        if self._approval_status is None:
            if self.approval is None:
                self._approval_status = gm.ChartStatus.PENDING.value
            else:
                self._approval_status = self.approval.status
        return self._approval_status

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
        if self.edited_in_staging is not None:
            return (not self.is_modified) and self.edited_in_staging
        else:
            log.error(
                "self.edited_in_staging is not set. Therefore, a True value here might reflect a chart that exists in staging and was deleted in production."
            )
            return not self.is_modified

    @property
    def is_deleted_in_target(self) -> bool:
        """Check if the chart is deleted in target."""
        if self.edited_in_staging is not None:
            return (not self.is_modified) and (not self.edited_in_staging)
        else:
            raise ValueError("This method is only implemented if self.edited_in_staging is defined.")

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
            # Only published charts have slugs
            if self.target_chart.publishedAt is not None:
                if self.source_chart.slug != self.target_chart.slug:
                    # This happens only when someone renames a slug in the target environment
                    log.error(
                        "Slug mismatch!",
                        source_chart_slug=self.source_chart.slug,
                        target_chart_slug=self.target_chart.slug,
                        chart_id=self.chart_id,
                    )
        return self.source_chart.slug or "no-slug"

    @property
    def in_conflict(self) -> bool:
        """Check if the chart in target is newer than the source."""
        if self._in_conflict is None:
            if self.target_chart is None:
                return False

            # Check if chart has been edited in production
            with Session(OWID_ENV.engine) as session:
                chart_edited_in_prod = self.target_chart.updatedAt > get_staging_creation_time(session)

            # If edited, check if conflict was resolved
            if chart_edited_in_prod:
                resolved = False
                if self.conflict is not None:
                    resolved = self.conflict.conflict == "resolved"
                self._in_conflict = chart_edited_in_prod & (not resolved)
            else:
                self._in_conflict = False
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
                # NOTE: configs might differ and edited_in_staging is False if the chart had just
                #   data / metadata changes
                if self.edited_in_staging and self.target_chart and not self.configs_are_equal():
                    self._change_types.append("config")

                # TODO: Should uncomment this maybe?
                # assert self._change_types != [], "No changes detected!"

        return self._change_types

    @classmethod
    def from_charts_df(
        cls, df_charts: pd.DataFrame, source_session: Session, target_session: Session
    ) -> List["ChartDiff"]:
        """Get chart diffs from chart ids.

        NOTE: I am a bit confused. I see we have df_charts, which has already some information on whether the
        metadata, data, config fields were edited. However, we estimate this again with all the checksums code. Couldn't we just use the info from df_charts?
        """

        # Return empty list if no chart difference is found
        if df_charts.empty:
            return []

        chart_ids = list(set(df_charts.index))

        # Get charts from SOURCE db and save them in memory as dictionaries: chart_id -> chart
        source_charts = gm.Chart.load_charts(source_session, chart_ids=chart_ids)
        source_charts = {chart.id: chart for chart in source_charts}
        assert len(source_charts) == len(chart_ids), "Length of lists should match!"

        # Get charts from TARGET db
        target_charts = cls._get_target_charts(target_session, source_charts)

        # Get approval status
        # approval_statuses = cls._get_approval_statuses(source_session, chart_ids, source_charts, target_charts)
        approvals = cls._get_approvals(source_session, chart_ids, source_charts, target_charts)

        # Get conflicts
        conflicts = cls._get_conflicts(source_session, chart_ids, target_charts)

        # Get checksums
        checksums_diff = cls._get_checksums(source_session, target_session, chart_ids)

        # Get all slugs from target
        slugs_in_target = cls._get_chart_slugs(target_session)

        # Build chart diffs
        chart_diffs = []
        for chart_id, source_chart in source_charts.items():
            # Get target chart (if exists)
            target_chart = target_charts.get(chart_id)

            ## Checks
            if target_chart:
                assert source_chart.createdAt == target_chart.createdAt, "CreatedAt mismatch!"

            # Approval
            assert chart_id in approvals, f"Approval not found for chart {chart_id}"
            approval = approvals[chart_id]

            # Conflict
            assert chart_id in approvals, f"Conflict not found for chart {chart_id}"
            conflict = conflicts[chart_id]

            # Checksums
            modified_checksum = checksums_diff.loc[chart_id] if target_chart else None

            # Was the chart edited in Staging?
            if chart_id in df_charts.index:
                edited_in_staging = df_charts.loc[chart_id, "chartEditedInStaging"]
            else:
                edited_in_staging = None

            # Are there any errors?
            # Creating new chart, but slug already exists in target
            if not target_chart and source_chart.slug in slugs_in_target:
                error = f"Slug '{source_chart.slug}' already exists in target environment."
            else:
                error = None

            # Build Chart Diff object
            chart_diff = cls(
                source_chart, target_chart, approval, conflict, modified_checksum, edited_in_staging, error
            )
            chart_diffs.append(chart_diff)

        return chart_diffs

    def get_all_approvals(self, session: Session) -> List[gm.ChartDiffApprovals]:
        """Get history of chart diff."""
        # Get history
        history = gm.ChartDiffApprovals.get_all(
            session,
            chart_id=self.chart_id,
        )
        return history

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
            # Update approval status (in database)
            assert self.chart_id
            if self.is_modified:
                assert self.target_chart
            approval = gm.ChartDiffApprovals(
                chartId=self.chart_id,
                sourceUpdatedAt=self.source_chart.updatedAt,
                targetUpdatedAt=None if self.is_new else self.target_chart.updatedAt,  # type: ignore
                status=status,  # type: ignore
            )
            session.add(approval)
            session.commit()

            # Add approval to object
            self.approval = approval

    def set_conflict_to_resolved(self, session: Session) -> None:
        """Update the state of the chart diff."""
        # Only perform action if status changes!
        assert self.is_modified, "Conflicts can only occur for modified charts!"
        # Update approval status (in database)
        assert self.chart_id
        if self.is_modified:
            assert self.target_chart
        conflict = gm.ChartDiffConflicts(
            chartId=self.chart_id,
            targetUpdatedAt=self.target_chart.updatedAt,  # type: ignore
            conflict="resolved",
        )
        session.add(conflict)
        session.commit()

        # Add conflict to object
        self.conflict = conflict

    def configs_are_equal(self) -> bool:
        """Compare two chart configs, ignoring version, id and isPublished."""
        assert self.target_chart is not None, "Target chart is None!"
        return configs_are_equal(self.source_chart.config, self.target_chart.config, verbose=False)

    @property
    def details(self):
        return {
            "chart_id": self.chart_id,
            "is_approved": self.is_approved,
            "is_pending": self.is_pending,
            "is_rejected": self.is_rejected,
            "is_reviewed": self.is_reviewed,
            "is_new": self.is_new,
            "error": self.error,
        }

    @staticmethod
    def _get_chart_slugs(target_session: Session) -> set[str]:
        slugs_redirects = set(read_sql("SELECT slug FROM chart_slug_redirects", target_session)["slug"])
        slugs = set(read_sql("SELECT slug FROM chart_configs", target_session)["slug"])
        return slugs | slugs_redirects

    @staticmethod
    def _get_target_charts(target_session, source_charts):
        """Get charts from TARGET environment.

        Note that we use source_charts to make sure that we are getting the right charts from TARGET.
        This is because some charts can be created in different environments independently and be different but
        share the same ID.
        """

        def _charts_are_equivalent_envs(s_chart, t_chart):
            # It can happen that both charts have the same ID, but are completely different (this
            # happens when two charts are created independently on two servers). If they
            # have same createdAt then they are the same chart.
            return not (t_chart and (s_chart.createdAt != t_chart.createdAt))

        chart_ids = source_charts.keys()

        if target_session is not None:
            try:
                target_charts = gm.Chart.load_charts(target_session, chart_ids=chart_ids)
            except NoResultFound:
                target_charts = {}
            else:
                target_charts = {
                    chart.id: chart if _charts_are_equivalent_envs(source_charts[chart.id], chart) else None
                    for chart in target_charts
                }
        else:
            target_charts = {}
        return target_charts

    @staticmethod
    def _get_approvals(
        source_session, chart_ids, source_charts, target_charts
    ) -> Dict[int, Optional[gm.ChartDiffApprovals]]:
        target_updated_ats = []
        for chart_id in chart_ids:
            if target_charts.get(chart_id) is not None:
                target_updated_ats.append(target_charts[chart_id].updatedAt)  # type: ignore
            else:
                target_updated_ats.append(None)
        approvals = gm.ChartDiffApprovals.latest_chart_approval_batch(
            source_session,
            chart_ids,
            [source_charts[chart_id].updatedAt for chart_id in chart_ids],
            target_updated_ats,
        )
        approvals = dict(zip(chart_ids, approvals))
        return approvals

    @staticmethod
    def _get_conflicts(source_session, chart_ids, target_charts) -> Dict[int, Optional[gm.ChartDiffConflicts]]:
        target_updated_ats = []
        for chart_id in chart_ids:
            if target_charts.get(chart_id) is not None:
                target_updated_ats.append(target_charts[chart_id].updatedAt)  # type: ignore
            else:
                target_updated_ats.append(None)
        conflicts = gm.ChartDiffConflicts.get_conflict_batch(
            source_session,
            chart_ids,
            target_updated_ats,
        )
        conflicts = dict(zip(chart_ids, conflicts))
        return conflicts

    @staticmethod
    def _get_checksums(source_session, target_session, chart_ids) -> pd.DataFrame:
        checksums_source = gm.Chart.load_variables_checksums(source_session, chart_ids)
        checksums_target = gm.Chart.load_variables_checksums(target_session, chart_ids)
        checksums_source, checksums_target = checksums_source.align(checksums_target)
        checksums_diff = checksums_source != checksums_target
        # If checksum has not been filled yet, assume unchanged
        checksums_diff[checksums_target.isna()] = False

        # If the chart was updated by indicator upgrader, then variables in both charts will have different
        # variable ids (and catalog paths). We don't want to label these as data or metadata changes, as they
        # will be labaled as config change (if the change was done on the staging server). This helps with
        # false positives when rebasing PRs.
        checksums_diff[checksums_source.isna()] = False

        return checksums_diff


class ChartDiffsLoader:
    """Detect charts that differ between staging and production and load them."""

    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.df = self.load_df()

        # Cache
        self._diffs: List[ChartDiff] | None = None

    def load_df(self, chart_ids: List[int] | None = None) -> pd.DataFrame:
        """Load changes in charts between environments from sessions."""
        with Session(self.source_engine) as source_session:
            with Session(self.target_engine) as target_session:
                return modified_charts_on_staging(source_session, target_session, chart_ids=chart_ids)

    @property
    def chart_ids_all(self):
        return set(self.df.index[self.df.any(axis=1)])

    def get_charts_df(
        self,
        config: bool | None = None,
        data: bool | None = None,
        metadata: bool | None = None,
    ) -> pd.DataFrame:
        """DataFrame with charts details."""
        return self.df[
            (self.df.configEdited & config) | (self.df.dataEdited & data) | (self.df.metadataEdited & metadata)
        ]

    def get_diffs(
        self,
        config: bool = True,
        data: bool = False,
        metadata: bool = False,
        sync: bool = False,
        chart_ids: Optional[List[int]] = None,
        source_session: Optional[Session] = None,
        target_session: Optional[Session] = None,
    ) -> List[ChartDiff]:
        """Optimised version of get_diffs."""
        if chart_ids:
            assert sync, "If chart_ids are provided, sync must be True."
        if sync:
            self.df = self.load_df(chart_ids=chart_ids)
        # Get ids of charts with relevant changes
        df_charts = self.get_charts_df(config, data, metadata)

        if source_session and target_session:
            chart_diffs = ChartDiff.from_charts_df(df_charts, source_session, target_session)
        else:
            with Session(self.source_engine) as source_session, Session(self.target_engine) as target_session:
                chart_diffs = ChartDiff.from_charts_df(df_charts, source_session, target_session)

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
        for diff in chart_diffs:
            summary.append(diff.details)
        return pd.DataFrame(summary)


def _modified_data_metadata_on_staging(
    source_session: Session, target_session: Session, chart_ids: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Get charts with modified data or metadata. This is done by taking all variables used in a chart from
    the source changed by the Admin user and comparing their checksums to the target. The key aspect is how we
    filter them - if a variable has been updated more recently on the target, we exclude it. This is not a perfect
    solution, but it prevents showing numerous outdated charts when the source is lagging behind the target (master).

    ISSUES:
    Some datasets like COVID or certain AI datasets use {TODAY} in their metadata, making the metadata dependent
    on the creation date. Merging a day later results in many metadata changes. The current workaround is to
    exclude these datasets from comparison, similar to what we do for data-diff.
    """
    # get modified variables that are used in charts
    base_q = """
    select
        v.catalogPath as catalogPath,
        cd.chartId,
        v.dataChecksum,
        v.metadataChecksum,
        d.dataEditedByUserId,
        d.dataEditedAt as dataLastEditedAt,
        d.metadataEditedByUserId,
        d.metadataEditedAt as metadataLastEditedAt
    from chart_dimensions as cd
    join variables as v on cd.variableId = v.id
    join datasets as d on v.datasetId = d.id
    where v.dataChecksum is not null and v.metadataChecksum is not null and
    """
    where = """
        -- only compare data/metadata that have been updated on staging server
        (d.dataEditedAt >= %(timestamp_staging_creation)s or d.metadataEditedAt >= %(timestamp_staging_creation)s)
    """
    query_source = base_q + where
    params = {"timestamp_staging_creation": get_staging_creation_time(source_session)}
    # Add filter for chart IDs
    if chart_ids is not None:
        where_charts = """
            -- filter and get only charts with given IDs
            cd.chartId in %(chart_ids)s
        """
        query_source += " and " + where_charts
        params["chart_ids"] = tuple(chart_ids)
    source_df = read_sql(query_source, source_session, params=params)

    # no charts, return empty dataframe
    if source_df.empty:
        return pd.DataFrame(columns=["chartId", "dataEdited", "metadataEdited"]).set_index("chartId")

    # Get all changed files and their catalog paths, including downstream dependencies.
    files_changed = get_changed_files()
    catalog_paths = get_all_changed_catalog_paths(files_changed)

    # Exclude variables that haven't been changed by updating the files. This is to prevent showing
    # spurious changes from lagging behind master.
    dataset_paths = source_df.catalogPath.str.split("/").str[:4].str.join("/")
    source_df = source_df[dataset_paths.isin(catalog_paths)]

    # no charts, return empty dataframe
    if source_df.empty:
        return pd.DataFrame(columns=["chartId", "dataEdited", "metadataEdited"]).set_index("chartId")

    # read those variables from target
    where = """
        v.catalogPath in %(catalog_paths)s
    """
    target_df = read_sql(
        base_q + where, target_session, params={"catalog_paths": tuple(source_df.catalogPath.unique())}
    )

    source_df = source_df.set_index(["chartId", "catalogPath"])
    target_df = target_df.set_index(["chartId", "catalogPath"])

    # align dataframes with INNER join
    # the inner join is on purpose, because we only want to compare variables that are used in both environments
    # if the variable is not present in target, it could mean that the chart was updated in target (but we don't
    # care about that, because it's not a data/metadata change, but chart config change)
    source_df, target_df = source_df.align(target_df, join="inner")

    # Only include variables with more recent update in source. If the variable has been updated in target, then
    # exclude it (typically an automatic update or source hasn't been merged with master and it's lagging behind it)
    ix = source_df.dataLastEditedAt >= target_df.dataLastEditedAt
    source_df = source_df[ix]
    target_df = target_df[ix]

    # Get differences
    diff = pd.DataFrame(
        {
            "dataEdited": source_df.dataChecksum != target_df.dataChecksum,
            "metadataEdited": source_df.metadataChecksum != target_df.metadataChecksum,
        }
    )

    # Exclude metadata changes for certain datasets
    for ex in EXCLUDE_METADATA_CHANGES:
        ix = diff.index.get_level_values("catalogPath").str.contains(ex)
        diff.loc[ix, "metadataEdited"] = False

    diff = diff.groupby("chartId").any()

    return diff


def _modified_chart_configs_on_staging(
    source_session: Session, target_session: Session, chart_ids: Optional[List[int]] = None
) -> pd.DataFrame:
    TIMESTAMP_STAGING_CREATION = get_staging_creation_time(source_session)

    # get modified charts
    base_q = """
    select
        c.id as chartId,
        MD5(cc.full) as chartChecksum,
        c.lastEditedByUserId as chartLastEditedByUserId,
        c.publishedByUserId as chartPublishedByUserId,
        c.lastEditedAt as chartLastEditedAt
    from charts as c
    join chart_configs as cc on c.configId = cc.id
    where
    """
    where = """
        -- only compare charts that have been updated on staging server
        (
            c.lastEditedAt >= %(timestamp_staging_creation)s
        )
    """
    query_source = base_q + where
    params = {"timestamp_staging_creation": TIMESTAMP_STAGING_CREATION}
    # Add filter for chart IDs
    if chart_ids is not None:
        where_charts = """
            -- filter and get only charts with given IDs
            c.id in %(chart_ids)s
        """
        query_source += " and " + where_charts
        params["chart_ids"] = tuple(chart_ids)
    source_df = read_sql(query_source, source_session, params=params)

    # no charts, return empty dataframe
    if source_df.empty:
        return pd.DataFrame(columns=["chartId", "configEdited", "chartEditedInStaging"]).set_index("chartId")

    # read those charts from target
    where = """
        c.id in %(chart_ids)s
    """
    target_df = read_sql(base_q + where, target_session, params={"chart_ids": tuple(source_df.chartId.unique())})

    source_df = source_df.set_index("chartId")
    target_df = target_df.set_index("chartId")

    # align dataframes with left join (so that source has non-null values)
    # NOTE: new charts will be already in source
    # NOTE: chart IDs and variable IDs in both environments do not necessarily correspond to the same charts or indicators! While there might be a chart with ID X in both environments that corresponds to different charts, it is rare that there is a chart with ID X using indicator with ID Y, that are different. However, this can't be ruled out. Therefore, aligning source_df and target_df by chartId and variableId can fail sometimes!
    source_df, target_df = source_df.align(target_df, join="left")

    diff = source_df.copy()
    diff["configEdited"] = source_df["chartChecksum"] != target_df["chartChecksum"]

    # Add flag 'edited in staging'
    diff["chartEditedInStaging"] = True

    # Remove charts with no changes
    return diff[["configEdited", "chartEditedInStaging"]]


def modified_charts_on_staging(
    source_session: Session, target_session: Session, chart_ids: Optional[List[int]] = None
) -> pd.DataFrame:
    """Get charts that have been modified in staging.

    - It includes charts with different config, data or metadata checksums.
    - It detects changes by comparing updatedAt timestamps to staging creation time.

    Optionally, you can provide a list of chart IDs to filter the results.

    The returned object is a dataframe with the following columns:
        - chartId (index): ID of the chart
        - dataEdited: True if data checksum has changed
        - metadataEdited: True if metadata checksum has changed
        - configEdited: True if config has changed

        TESTING:
        - chartEditedInStaging: True if the chart config has been edited in staging.
    """
    df_config = _modified_chart_configs_on_staging(source_session, target_session, chart_ids=chart_ids)
    df_data_metadata = _modified_data_metadata_on_staging(source_session, target_session, chart_ids=chart_ids)

    df = df_config.join(df_data_metadata, how="outer").fillna(False)

    return df


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
    )

    chart_diffs = {chart.chart_id: chart for chart in chart_diffs}

    return chart_diffs


def configs_are_equal(config_1: Dict[str, Any], config_2: Dict[str, Any], verbose=False) -> bool:
    """Compare two chart configs, ignoring certain fields."""
    exclude_keys = ("id", "isPublished", "bakedGrapherURL", "adminBaseUrl", "dataApiUrl", "version")
    config_1 = {k: v for k, v in config_1.items() if k not in exclude_keys}
    config_2 = {k: v for k, v in config_2.items() if k not in exclude_keys}

    # Use pretty print to convert dicts to strings for comparison
    config_1_str = pprint.pformat(config_1, sort_dicts=True)
    config_2_str = pprint.pformat(config_2, sort_dicts=True)

    # Compare the string representations using difflib
    if config_1_str == config_2_str:
        return True

    if verbose:
        log.warning("Configurations differ")
        diff = difflib.unified_diff(config_1_str.splitlines(), config_2_str.splitlines(), lineterm="")

        # Print the diff
        for line in diff:
            print(line)

    return False
