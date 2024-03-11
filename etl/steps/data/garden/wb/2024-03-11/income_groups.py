"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

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
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("income_groups")
    tb = ds_meadow["income_groups"].reset_index()

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

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create an additional table for the classification of the latest year available.
    tb_latest = tb.reset_index().drop_duplicates(subset=["country"], keep="last")

    # Rename new table.
    tb_latest.metadata.short_name = "income_groups_latest"

    # Check that countries without classification for the latest year are as expected.
    missing_countries = set(tb_latest.loc[tb_latest["year"] != tb_latest["year"].max(), "country"])
    assert (
        missing_countries == EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE
    ), f"Unexpected missing countries in latest release. All missing countries: {missing_countries}"

    # Extract data only for latest release (and remove column year).
    tb_latest = tb_latest[tb_latest["year"] == tb_latest["year"].max()].drop(columns=["year"])

    # Set an appropriate index and sort conveniently.
    tb_latest = tb_latest.set_index(["country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_latest], default_metadata=ds_meadow.metadata)
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
        np.nan,
    }, f"Unknown income group label! Check {labels}"


def harmonize_income_group_labels(tb: Table) -> Table:
    # Check if unusual LM* label is still used for Yemen in 1987 and 1988.
    msk = tb["classification"] == "LM*"
    lm_special = set(tb[msk]["country_code"].astype(str) + tb[msk]["year"].astype(str))
    assert lm_special == {"YEM1987", "YEM1988"}, f"Unexpected entries with classification 'LM*': {tb[msk]}"

    # Rename labels.
    classification_mapping = {
        "..": np.nan,
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
