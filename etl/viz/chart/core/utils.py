from typing import Any, overload

from etl.viz.chart.model.core import Chart
from etl.viz.chart.utils import get_tables_by_name_mapping
from etl.viz.explorer import Explorer


def process_views(
    chart: Chart | Explorer,
    dependencies: set[str],
    combine_metadata_when_mult: bool = False,
):
    """Process views in Chart configuration."""
    # Resolve short-form catalog paths to full paths. Chart.save() does this again
    # before validation; doing it here too means anything that reads view paths between
    # create_chart and save sees full paths.
    tables_by_name = get_tables_by_name_mapping(dependencies)

    for view in chart.views:
        # Merge common_views FIRST so any short-form paths it injects
        # (e.g. `colorVariableId: regions#owid_region`) are visible to expand_paths
        # below. The opposite order leaves merged paths un-expanded — the failure
        # surfaces later at `combine_charts` time where dependencies are empty.
        if (chart.definitions is not None) and (chart.definitions.common_views is not None):
            view.combine_with_common(chart.definitions.common_views)
        else:
            # Validate even when there are no common_views to merge — the view's own config
            # might have incompatible settings
            view.validate_color_scale_config()

        # Expand short-form catalog paths in the (now-merged) view config and indicators.
        view.expand_paths(tables_by_name)

        # Combine metadata in views which contain multiple indicators
        if combine_metadata_when_mult and view.metadata_is_needed:  # Check if view "contains multiple indicators"
            # TODO
            # view["metadata"] = build_view_metadata_multi(indicators, tables_by_uri)
            # log.info(
            #     f"View with multiple indicators detected. You should edit its `metadata` field to reflect that! This will be done programmatically in the future. Check view with dimensions {view.dimensions}"
            # )
            pass


@overload
def create_chart_from_config(
    config: dict[str, Any],
    dependencies: set[str],
    catalog_path: str,
    *,  # Force keyword-only arguments after this
    dependencies_combined: set[str] | None = None,
    validate_schema: bool = True,
    explorer: bool,
) -> Explorer: ...


@overload
def create_chart_from_config(
    config: dict[str, Any],
    dependencies: set[str],
    catalog_path: str,
    *,  # Force keyword-only arguments after this
    dependencies_combined: set[str] | None = None,
    validate_schema: bool = True,
    explorer: bool = False,
) -> Chart: ...


def create_chart_from_config(
    config: dict[str, Any],
    dependencies: set[str],
    catalog_path: str,
    *,  # Force keyword-only arguments after this
    dependencies_combined: set[str] | None = None,
    validate_schema: bool = True,
    explorer: bool = False,
) -> Explorer | Chart:
    """Create a Chart or Explorer instance from a configuration dictionary.

    config: Configuration of the chart.
    dependencies: Set of dependencies (dataset URIs) for the chart.
    catalog_path: Path to the step.
    dependencies_combined: Optional set of combined dependencies.
    validate_schema: Whether to validate the schema of the chart.
    explorer: Whether to create an Explorer instance instead of a Chart.
    """
    # Read config as structured object
    if explorer:
        c = Explorer.from_dict(dict(**config, catalog_path=catalog_path))
    else:
        c = Chart.from_dict(dict(**config, catalog_path=catalog_path))

    # Edit views
    process_views(c, dependencies=dependencies)

    # Require a schema pin before `validate_schema()`, which also enforces it (`required` in
    # multidim-schema.json) but only reports the missing key — not how to fill it in.
    c.validate_grapher_schema_pinned()

    # Validate config
    if validate_schema:
        c.validate_schema()

    # Ensure that all views are in choices
    c.validate_views_with_dimensions()

    # Validate duplicate views
    c.check_duplicate_views()

    # Add dependencies to chart
    c.dependencies = dependencies_combined or dependencies

    return c
