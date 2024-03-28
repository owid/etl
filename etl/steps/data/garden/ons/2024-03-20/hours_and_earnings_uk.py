"""Load a meadow dataset and create a garden dataset."""

from typing import Dict

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INDICATOR_NAMES = {
    "10% earned less than": "p10",
    "10% earned more than": "p90",
    "25% earned less than": "p25",
    "25% earned more than": "p75",
    "50% earned less than": "median",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hours_and_earnings_uk")

    # Read table from meadow dataset.
    tb = ds_meadow["hours_and_earnings_uk"].reset_index()

    #
    # Process data.
    tb = calculate_ratios(tb, INDICATOR_NAMES)

    # Format year to int
    tb["year"] = tb["year"].str[:4].astype(int)
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.format(["aggregation", "country", "year", "spell"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_ratios(tb: Table, indicator_names: Dict[str, str]) -> Table:
    """Calculate ratios between indicators and median."""

    # Rename values in indicator column
    tb["indicator"] = tb["indicator"].replace(indicator_names)

    # Make table wide with indicator as columns
    tb_pivot = (
        tb.pivot_table(index=["country", "year", "spell", "aggregation"], columns="indicator", values="value")
        .reset_index()
        .copy()
    )

    # Copy metadata
    for col in indicator_names.values():
        tb_pivot[col] = tb_pivot[col].copy_metadata(tb["indicator"])

    # Calculate ratios
    tb_pivot["p90_p50_ratio"] = tb_pivot["p90"] / tb_pivot["median"] * 100
    tb_pivot["p90_p10_ratio"] = tb_pivot["p90"] / tb_pivot["p10"] * 100
    tb_pivot["p50_p10_ratio"] = tb_pivot["median"] / tb_pivot["p10"] * 100

    # Remove columns named as the indicator_names.values()
    tb_pivot = tb_pivot.drop(columns=indicator_names.values())

    return tb_pivot
