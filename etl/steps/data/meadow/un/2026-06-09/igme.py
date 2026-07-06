"""Load a snapshot and create a meadow dataset."""

import structlog

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = structlog.get_logger()


COLUMNS_TO_KEEP_MAP = {
    "Geographic area": "country",
    "Regional group": "regional_group",
    "Indicator": "indicator",
    "Sex": "sex",
    "Wealth Quintile": "wealth_quintile",
    "Series Name": "series_name",
    "Series Year": "series_year",
    "REF_DATE": "year",
    "OBS_VALUE": "observation_value",
    "LOWER_BOUND": "lower_bound",
    "UPPER_BOUND": "upper_bound",
    "Unit of measure": "unit_of_measure",
    "Age Group of Women": "age_group_women",
    "Time Since First Birth": "time_since_first_birth",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("igme.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("UN IGME 2025.csv", low_memory=False, safe_types=False)
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # Note: The 2025 release renamed several columns to SDMX-style uppercase names.
    tb = tb.rename(columns=COLUMNS_TO_KEEP_MAP, errors="raise")
    tb = tb[[col for col in COLUMNS_TO_KEEP_MAP.values() if col in tb.columns]]

    # Only grab the UN IGME estimates (not the input raw data)
    tb = tb[tb["series_name"] == "UN IGME estimate"]

    # remove unicef regions (there are duplicates for the unicef regions and we mostly care about country-level data)
    tb = tb[tb["regional_group"] != "UNICEF"]

    tb = tb.format(
        [
            "country",
            "year",
            "indicator",
            "sex",
            "wealth_quintile",
            "series_name",
            "regional_group",
            "unit_of_measure",
        ]
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata, repack=False
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
