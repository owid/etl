"""Load a meadow dataset and create a garden dataset."""

from typing import Dict

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INDICATOR_NAMES = {
    "Percentage of GDP": "share_gdp",
    "Percentage of expenditure on health": "share_expenditure",
    "US dollars per person, PPP converted": "ppp_dollars_per_capita",
    "US dollars, PPP converted": "ppp_dollars",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("health_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow["health_expenditure"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Make the indicators wide
    tb = make_indicators_wide(tb, INDICATOR_NAMES)

    # Transform health expenditure, saved originally in millions of dollars
    tb["ppp_dollars"] *= 1e6

    tb = tb.set_index(["country", "year", "financing_scheme"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_indicators_wide(tb: Table, indicator_names: Dict[str, str]) -> Table:
    """
    This function makes the indicators wide
    """
    # Rename the indicators
    tb["indicator"] = tb["indicator"].map(indicator_names)

    # Make the indicators wide
    tb = tb.pivot(
        index=["country", "year", "financing_scheme"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Make share_expenditure null when financing_scheme is "Total"
    tb.loc[tb["financing_scheme"] == "Total", "share_expenditure"] = None

    return tb
