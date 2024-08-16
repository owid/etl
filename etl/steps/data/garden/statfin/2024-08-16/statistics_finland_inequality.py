"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

GINI_COLUMNS = {
    "year": "year",
    "disposable_cash_income__excl__capital_gains__cross_nationally_comparable_concept__sample_based_data": "gini",
}

RELATIVE_POVERTY_COLUMNS = {
    "year": "year",
    "at_risk_of_poverty_rate__threshold_60_pct_of_median": "relative_poverty",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("statistics_finland_inequality")

    # Read table from meadow dataset.
    tb_gini = ds_meadow["gini_coefficient"].reset_index()
    tb_rel = ds_meadow["relative_poverty"].reset_index()

    #
    # Process data.
    #
    tb = merge_tables_and_format(tb_gini, tb_rel)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def merge_tables_and_format(tb_gini: Table, tb_rel: Table) -> Table:
    """
    Merge gini and relative poverty tables, delete unnecessary columns, and format the table.
    """
    # Keep necessary columns and rename them.
    tb_gini = tb_gini[GINI_COLUMNS.keys()].rename(columns=GINI_COLUMNS)
    tb_rel = tb_rel[RELATIVE_POVERTY_COLUMNS.keys()].rename(columns=RELATIVE_POVERTY_COLUMNS)

    # Merge tables.
    tb = pr.merge(tb_gini, tb_rel, on=["year"], how="outer", short_name=paths.short_name)

    # Add country column.
    tb["country"] = "Finland"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    return tb
