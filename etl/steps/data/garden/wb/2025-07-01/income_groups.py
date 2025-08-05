"""Load a meadow dataset and create a garden dataset."""
# NOTE: We have manually modified the value for Ethiopia, because, although it is included in the file, it has officially a temporary status of unclassification.
# NOTE: Check this back when it's fixed in the source file.

from typing import List

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE = {
    "Czechoslovakia",
    "Mayotte",
    "Netherlands Antilles",
    "Serbia and Montenegro",
    "USSR",
    "Venezuela",
    "Yugoslavia",
    "Ethiopia",  # NOTE: This is the one we manually modified. Delete when it has a classification again.
}

# Define French overseas territories where we want to assign the same income group as France
FRENCH_OVERSEAS_TERRITORIES = [
    "French Guiana",
    "French Southern Territories",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("income_groups")
    tb = ds_meadow.read("income_groups")

    #
    # Process data.
    #
    # Run sanity checks on input data.
    run_sanity_checks_on_inputs(tb=tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Harmonize income group labels.
    tb = harmonize_income_group_labels(tb)

    # Drop unnecessary columns.
    tb = tb.drop(columns=["country_code"], errors="raise")

    # Delete the row for Ethiopia in the latest year, as it has a temporary status of unclassification.
    # NOTE: This is a manual fix, delete the line when the source file is fixed.
    tb = tb[~((tb["country"] == "Ethiopia") & (tb["year"] == tb["year"].max()))].reset_index(drop=True)

    # Create an additional table for the classification of the latest year available.
    tb_latest = tb.reset_index(drop=True).drop_duplicates(subset=["country"], keep="last")

    # Check that countries without classification for the latest year are as expected.
    missing_countries = set(tb_latest.loc[tb_latest["year"] != tb_latest["year"].max(), "country"])
    assert (
        missing_countries == EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE
    ), f"Unexpected missing countries in latest release. All missing countries: {missing_countries}"

    # Extract data only for latest release (and remove column year).
    tb_latest = tb_latest[tb_latest["year"] == tb_latest["year"].max()].drop(columns=["year"])

    # Assign the same income group as France to the French overseas territories.
    tb = assign_french_overseas_group_same_as_france(
        tb=tb,
        list_of_territories=FRENCH_OVERSEAS_TERRITORIES,
    )
    tb_latest = assign_french_overseas_group_same_as_france(
        tb=tb_latest,
        list_of_territories=FRENCH_OVERSEAS_TERRITORIES,
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"])

    # Set an appropriate index and sort conveniently.
    tb_latest = tb_latest.format(["country"], short_name="income_groups_latest")

    # Find the version of the current World Bank's classification.
    origin = tb_latest["classification"].metadata.origins[0]
    assert origin.producer == "World Bank", "Unexpected list of origins."
    year_world_bank_classification = origin.date_published.split("-")[0]

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_latest],
        default_metadata=ds_meadow.metadata,
        yaml_params={"year_world_bank_classification": year_world_bank_classification},
    )
    ds_garden.save()


def run_sanity_checks_on_inputs(tb: Table) -> None:
    # Check that raw labels are as expected.
    assert (labels := set(tb["classification"])) == {
        # No available classification for country-year (maybe country didn't exist yet/anymore).
        "..",
        # High income.
        "H",
        # Low income.
        "L",
        # Lower middle income.
        "LM",
        # Exceptional case of lower middle income.
        "LM*",
        # Upper middle income.
        "UM",
        # Another label for when no classification is available.
        pd.NA,
    }, f"Unknown income group label! Check {labels}"


def harmonize_income_group_labels(tb: Table) -> Table:
    # Check if unusual LM* label is still used for Yemen in 1987 and 1988.
    msk = tb["classification"] == "LM*"
    lm_special = set(tb[msk]["country_code"].astype(str) + tb[msk]["year"].astype(str))
    assert lm_special == {"YEM1987", "YEM1988"}, f"Unexpected entries with classification 'LM*': {tb[msk]}"

    # Rename labels.
    classification_mapping = {
        "..": pd.NA,
        "L": "Low-income countries",
        "H": "High-income countries",
        "UM": "Upper-middle-income countries",
        "LM": "Lower-middle-income countries",
        "LM*": "Lower-middle-income countries",
    }
    tb["classification"] = tb["classification"].map(classification_mapping)

    # Drop years with no country classification
    tb = tb.dropna(subset="classification").reset_index(drop=True)

    return tb


def assign_french_overseas_group_same_as_france(tb: Table, list_of_territories: List[str]) -> Table:
    """
    Assign the same income group as France to the French overseas territories.
    """

    tb = tb.copy()

    # Filter the rows where we have data for France
    tb_france = tb[tb["country"] == "France"].reset_index(drop=True)

    # # Keep only the columns we need
    # tb_france = tb_france[["year", "classification"]]

    tb_french_overseas = Table()

    for territory in list_of_territories:
        tb_territory = tb_france.copy()

        # Add country
        tb_territory["country"] = territory

        # Concatenate the two tables
        tb_french_overseas = pr.concat([tb_french_overseas, tb_territory], ignore_index=True)

    # Concatenate the two tables
    tb = pr.concat([tb, tb_french_overseas], ignore_index=True)

    return tb
