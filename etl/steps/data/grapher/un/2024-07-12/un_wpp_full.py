from typing import cast

from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Exclude the last three as we want to treat them differently and remove the january rows from them
TABLES_EXCLUDE = ["fertility_single", "population", "sex_ratio", "dependency_ratio"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")

    #
    # Process data.
    #
    tables = [reshape_table(ds_garden[tb_name]) for tb_name in ds_garden.table_names if tb_name not in TABLES_EXCLUDE]
    # Fiona: Removing the January data for now, we want it for Cologne but no need to change the live data for now.
    tb_pop = remove_jan_data(ds_garden["population"])
    tb_sex_ratio = remove_jan_data(ds_garden["sex_ratio"])
    tb_dependency = remove_jan_data(ds_garden["dependency_ratio"])
    tables.append(tb_pop)
    tables.append(tb_sex_ratio)
    tables.append(tb_dependency)
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


def remove_jan_data(tb: Table) -> Table:
    tb = reshape_table(tb)
    tb = tb.reset_index()
    tb = tb[tb["month"].isin(["july", "July"])].drop(columns=["month"]).copy()
    tb = tb.format(["country", "year", "sex", "age", "variant"])
    return tb


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
