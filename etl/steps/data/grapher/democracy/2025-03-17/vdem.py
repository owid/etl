"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("vdem")

    # Read tables
    tables = {}
    for tname in ds_garden.table_names:
        tables[tname] = ds_garden[tname]

    # tb_num_countries = ds_garden2["vdem_num_countries"]
    # tb_population = ds_garden2["vdem_population"]

    #
    # Process data.
    #
    ## Special indicator values renamings
    ## We add labels for these indicators. Otherwise they'd have the value zero (which is correct). Labelling makes them more informative.
    tables["vdem"] = add_categories_to_counters(tables["vdem"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    tables = list(tables.values())

    # Create grapher dataset
    ds_grapher = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def add_categories_to_counters(tb: Table) -> Table:
    tb["num_years_in_electdem_consecutive"] = tb["num_years_in_electdem_consecutive"].astype("string")
    tb.loc[tb["regime_row_owid"] == 0, "num_years_in_electdem_consecutive"] = "closed autocracy"
    tb.loc[tb["regime_row_owid"] == 1, "num_years_in_electdem_consecutive"] = "electoral autocracy"

    tb["num_years_in_libdem_consecutive"] = tb["num_years_in_libdem_consecutive"].astype("string")
    tb.loc[tb["regime_row_owid"] == 0, "num_years_in_libdem_consecutive"] = "closed autocracy"
    tb.loc[tb["regime_row_owid"] == 1, "num_years_in_libdem_consecutive"] = "electoral autocracy"
    tb.loc[tb["regime_row_owid"] == 2, "num_years_in_libdem_consecutive"] = "electoral democracy"
    return tb
