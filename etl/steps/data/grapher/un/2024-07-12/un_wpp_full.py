from typing import cast

from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Exclude tables that don't fit the estimates-into-each-variant reshape: fertility_single and
# population_january are treated differently, and population_peak has no variant dimension.
TABLES_EXCLUDE = ["fertility_single", "population_january", "population_peak"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")

    #
    # Process data.
    #
    tables = []
    for tb_name in ds_garden.table_names:
        # print(tb_name)
        if tb_name not in TABLES_EXCLUDE:
            tb_new = reshape_table(ds_garden[tb_name])
            tables.append(tb_new)

    # Edit title
    ds_garden.metadata.title = cast(str, ds_garden.metadata.title) + " (projections full timeseries)"

    #
    # Save outputs.
    #
    # Create grapher dataset
    ds_grapher = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def reshape_table(tb: Table) -> Table:
    index_names = tb.index.names
    tb = tb.reset_index()

    # Filter estimates vs projections
    mask = tb["variant"] == "estimates"
    tb_estimates = tb.loc[mask].copy()
    tb = tb.loc[~mask].copy()

    # Projection scenarios
    # variant_names = {"low", "medium", "high"}
    variant_names = set(tb["variant"]) - {"estimates"}

    # Build table
    tb = concat(
        [
            tb,
            *[tb_estimates.assign(variant=variant) for variant in variant_names],
        ]
    )

    return tb.format(index_names)
