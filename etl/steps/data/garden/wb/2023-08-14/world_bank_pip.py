"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_data(tb: Table) -> Table:
    # Calculate number in poverty
    tb["headcount"] = tb["headcount_ratio"] * tb["reporting_pop"]
    tb["headcount"] = tb["headcount"].round(0)

    # Calculate shortfall of incomes
    tb["total_shortfall"] = tb["poverty_gap_index"] * tb["poverty_line"] * tb["reporting_pop"]

    # Calculate average shortfall of incomes (averaged across population in poverty)
    tb["avg_shortfall"] = tb["total_shortfall"] / tb["headcount"]

    # Calculate income gap ratio (according to Ravallion's definition)
    tb["income_gap_ratio"] = (tb["total_shortfall"] / tb["headcount"]) / tb["poverty_line"]

    # Shares to percentages
    # executing the function over list of vars
    pct_indicators = ["headcount_ratio", "income_gap_ratio", "poverty_gap_index"]
    tb.loc[:, pct_indicators] = tb[pct_indicators] * 100

    # Create a new column for the poverty line in cents and string
    tb["poverty_line_cents"] = (tb["poverty_line"] * 100).astype(int).astype(str)

    # Make the table wide, with poverty_line_cents as columns
    tb = tb.pivot(
        index=[
            "ppp_version",
            "country",
            "year",
            "reporting_level",
            "welfare_type",
            "reporting_pop",
            "mean",
            "median",
            "mld",
            "gini",
            "polarization",
            "decile1",
            "decile2",
            "decile3",
            "decile4",
            "decile5",
            "decile6",
            "decile7",
            "decile8",
            "decile9",
            "decile10",
            "is_interpolated",
            "distribution_type",
            "estimation_type",
        ],
        columns="poverty_line_cents",
        values=[
            "headcount",
            "headcount_ratio",
            "income_gap_ratio",
            "poverty_gap_index",
            "avg_shortfall",
            "total_shortfall",
        ],
    )

    # Flatten column names
    tb.columns = ["_".join(col).strip() for col in tb.columns.values]

    # Reset index
    tb = tb.reset_index()

    # Changing the decile(i) variables for decile(i)_share
    for i in range(1, 11):
        tb = tb.rename(columns={f"decile{i}": f"decile{i}_share"})

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("world_bank_pip"))

    # Read table from meadow dataset.
    tb = ds_meadow["world_bank_pip"].reset_index()

    #
    # Process data.

    # rename columns
    tb = tb.rename(columns={"headcount": "headcount_ratio", "poverty_gap": "poverty_gap_index"})

    # Process data: Make table wide and change column names
    tb = process_data(tb)

    #
    # NOTE: Separate income and consumption data.

    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.set_index(["ppp_version", "country", "year", "reporting_level", "welfare_type"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
