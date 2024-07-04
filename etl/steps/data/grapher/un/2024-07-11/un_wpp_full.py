from typing import cast

from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")
    #
    # Process data.
    #
    tables = [reshape_table(ds_garden[tb_name]) for tb_name in ds_garden.table_names]

    # Edit title
    ds_garden.metadata.title = cast(str, ds_garden.metadata.title) + " (projections full timeseries)"

    #
    # Save outputs.
    #
    # Create grapher dataset
    ds_grapher = create_dataset(
        dest_dir,
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
    variant_names = {"medium"}

    # Build table
    tb = concat(
        [
            tb,
            *[tb_estimates.assign(variant=variant) for variant in variant_names],
        ]
    )

    return tb.format(index_names)
