"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MEASURE_NAMES = {
    "A": "intensity",
    "H": "headcount_ratio",
    "M0": "mpi",
    "hd": "uncensored_headcount_ratio",
    "hdk": "censored_headcount_ratio",
    "sev": "severe",
    "vuln": "vulnerable",
}

# Define categories to keep in each column
CATEGORIES_TO_KEEP = {
    "loa": ["area", "nat"],
    "measure": list(MEASURE_NAMES.keys()),
}

# Define indicator categories
INDICATOR_NAMES = [
    "Assets",
    "Child mortality",
    "Cooking fuel",
    "Drinking water",
    "Electricity",
    "Housing",
    "Nutrition",
    "Sanitation",
    "School attendance",
    "Years of schooling",
]

# Define index column for the final table
INDEX_COLS = ["country", "year", "indicator", "area", "flavor"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("multidimensional_poverty_index")

    # Read table from meadow dataset.
    tb_hot = ds_meadow["hot"].reset_index()
    tb_cme = ds_meadow["cme"].reset_index()

    #
    # Process data.
    #
    tb_hot = geo.harmonize_countries(
        df=tb_hot,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )
    tb_cme = geo.harmonize_countries(
        df=tb_cme,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    tb = make_tables_wide_and_merge(tb_cme=tb_cme, tb_hot=tb_hot)

    tb = tb.format(keys=INDEX_COLS, short_name="multidimensional_poverty_index")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_tables_wide_and_merge(tb_cme: Table, tb_hot: Table) -> Table:
    """
    Make tables wide to separate indicators, rename categories and merge hot and cme tables
    """

    tb_cme = select_categories_and_rename(tb_cme)
    tb_hot = select_categories_and_rename(tb_hot)

    # Make tables wide
    tb_hot = tb_hot.pivot(
        index=["country", "year", "indicator", "area"],
        columns=["measure"],
        values="b",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    tb_cme = tb_cme.pivot(
        index=["country", "year", "indicator", "area"],
        columns=["measure"],
        values="b",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    # Add a flavor column to each table
    tb_cme["flavor"] = "Current margin estimate"
    tb_hot["flavor"] = "Harmonized over time"

    # Concatenate the two tables
    tb = pr.concat([tb_cme, tb_hot], ignore_index=True)

    return tb


def select_categories_and_rename(tb: Table) -> Table:
    """
    Select categories to keep and rename them
    """

    for col, categories in CATEGORIES_TO_KEEP.items():
        # Assert that all categories are in the column
        assert set(categories).issubset(
            set(tb[col].unique())
        ), f"Categories {set(categories) - set(tb[col].unique())} not in column {col}"

        # Filter categories
        tb = tb[tb[col].isin(categories)].reset_index(drop=True)

    # Rename measure categories
    tb["measure"] = tb["measure"].cat.rename_categories(MEASURE_NAMES)

    # Check that the column ind_lab contains all INDICATOR_NAMES
    indicators_excluding_nan = tb[tb["ind_lab"].notna()]["ind_lab"].unique()
    assert (
        set(indicators_excluding_nan) == set(INDICATOR_NAMES)
    ), f"Column ind_lab is not identical to the expected list. These are the differences: {set(INDICATOR_NAMES) - set(indicators_excluding_nan)}"

    # Remove indicator and area columns
    tb = tb.drop(columns=["indicator"])

    # Rename ind_lab as indicator and area_lab as area
    tb = tb.rename(columns={"ind_lab": "indicator", "area_lab": "area"})

    return tb
