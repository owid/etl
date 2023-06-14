"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS = {
    "country": "country",
    "trade_off_landscape": "yield_gap_versus_nitrogen_pollution_effect",
}


def run(dest_dir: str) -> None:
    log.info("wuepper_et_al_2020.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("wuepper_et_al_2020"))

    # Read table from meadow dataset.
    tb = ds_meadow["wuepper_et_al_2020"]

    #
    # Process data.
    #
    # Drop duplicates (the row for Denmark is duplicated, where all columns have the exact same value, which is zero).
    tb = tb.drop_duplicates().reset_index(drop=True)

    # Select necessary columns and rename them.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS)

    # Convert column into a percentage.
    tb["yield_gap_versus_nitrogen_pollution_effect"] *= 100

    # Harmonize country names.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add a year column, assuming the year is the one when the paper was published.
    tb["year"] = 2020

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wuepper_et_al_2020.end")
