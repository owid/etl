"""The functions below are a bit more specific to this step, so maybe harder to generalize."""

from typing import Any, cast

from etl.collection.explorer import Explorer
from etl.helpers import PathFinder

# Projection variants in UN WPP. Order matters: it's the order in which y-indicators
# are concatenated when grouping with estimates. Estimates come first (solid line),
# projection second (dashed via `isProjection` metadata).
PROJECTION_VARIANTS = ["medium", "low", "high"]


class ExplorerCreator:
    """This class is particular to this step.

    This step relies on two datasets that are particular. One contains just estimates (1950-2023), and the other contains projections (1950-2100).
    """

    def __init__(self, paths, ds, ds_proj):
        self.paths: PathFinder = paths
        self.ds = ds
        self.ds_proj = ds_proj
        self.tbs = {"proj": {}, "estimates": {}}

    @property
    def all_tables(self):
        return [tt for t in self.tbs.values() for tt in t.values()]

    def table(self, table_name: str):
        if table_name not in self.tbs:
            self.tbs["estimates"][table_name] = self.ds.read(table_name, load_data=False)
        return self.tbs["estimates"][table_name]

    def table_proj(self, table_name: str):
        if table_name not in self.tbs:
            self.tbs["proj"][table_name] = self.ds_proj.read(table_name, load_data=False)
        return self.tbs["proj"][table_name]

    def create_manual(self, config: dict[str, Any], **kwargs) -> Explorer:
        explorer = self.paths.create_collection(
            config=config,
            indicator_as_dimension=True,
            explorer=True,
            **kwargs,
        )
        return cast(Explorer, explorer)

    def create(
        self,
        table_name: str,
        dimensions: dict[str, list[str] | str],
        dimensions_proj: dict[str, list[str] | str] | None = None,
        **kwargs,
    ) -> Explorer:
        """Creates an explorer based on `tb` (1950-2023) and `tb_proj` (1950-2100)."""
        self.paths.log.info(f"Creating explorer for {table_name}")

        if "config" not in kwargs:
            raise ValueError("The config is required to create the explorer. Please provide it in the kwargs.")

        # Load tables
        tb = self.table(table_name)
        tb_proj = self.table_proj(table_name)

        # Explorer with projections
        dimensions_ = {**dimensions, **(dimensions_proj or {"variant": ["medium", "high", "low"]})}

        explorer = self.paths.create_collection(
            tb=[tb, tb_proj],
            dimensions=[dimensions, dimensions_],
            indicator_as_dimension=True,
            explorer=True,
            **kwargs,
        )

        return explorer

    def create_with_grouped_projections(
        self,
        table_name: str,
        dimensions: dict[str, list[str] | str],
        projection_variants: list[str] = PROJECTION_VARIANTS,
        **kwargs,
    ) -> Explorer:
        """Create an explorer where projection views show estimates + projection as two y-indicators.

        This builds views from `un_wpp` alone (no `un_wpp_full`) for all four variants
        (estimates, medium, low, high), then groups each `[estimates, projection_variant]`
        pair into a combined view under the projection variant slug. Estimates-only views
        remain untouched.

        Requires that the `variant` variable-level metadata sets `isProjection` appropriately
        (e.g. via Jinja template on `variant`). The grapher renders the projection indicator
        with a dashed line while sharing the same color per entity as the estimates
        indicator (see `autoDetectSeriesStrategy`). Only works when each view ends up with
        exactly two y-indicators.
        """
        self.paths.log.info(f"Creating explorer (grouped projections) for {table_name}")

        if "config" not in kwargs:
            raise ValueError("The config is required to create the explorer. Please provide it in the kwargs.")

        if "variant" in dimensions:
            raise ValueError("`variant` should not be in `dimensions`; it is set by `create_with_grouped_projections`.")

        # Load only the estimates+projections table (has all four variants as separate series)
        tb = self.table(table_name)

        # Include all four variants as separate single-indicator views
        dimensions_all = {**dimensions, "variant": ["estimates", *projection_variants]}

        explorer = self.paths.create_collection(
            tb=tb,
            dimensions=dimensions_all,
            indicator_as_dimension=True,
            explorer=True,
            **kwargs,
        )

        # Combine [projection_variant, estimates] → projection_variant (two y-indicators).
        # The order matters: grapher's default ``map.columnSlug`` is y[0]. With the
        # projection column at y[0] and ``isProjection=true`` set on it, the grapher's
        # ``projectionColumnInfoBySlug`` auto-match pairs it with the estimates column at
        # y[1] and the map tab shows the full 1950–2100 series (historical +
        # projection combined). Putting estimates first instead would limit the map
        # to 1950–2023.
        # overwrite_dimension_choice=True replaces the existing single-indicator projection views.
        # replace=False (default) keeps the estimates-only views.
        # drop_dimensions_if_single_choice=False keeps single-valued dimensions such as
        # `indicator` (when there's a single `indicator_names`) or `age` (e.g. `["all"]`
        # for median_age). The `edit_views_*` helpers rely on those dimensions being
        # present on every view.
        explorer.group_views(
            groups=[
                {
                    "dimension": "variant",
                    "choices": [v, "estimates"],
                    "choice_new_slug": v,
                    "overwrite_dimension_choice": True,
                }
                for v in projection_variants
            ],
            drop_dimensions_if_single_choice=False,
        )

        return cast(Explorer, explorer)
