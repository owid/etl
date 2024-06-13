"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS = {
    "country": "country",
    "trade_off_landscape": "yield_gap_versus_nitrogen_pollution_effect",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("wuepper_et_al_2020")
    tb = ds_meadow["wuepper_et_al_2020"].reset_index()

    #
    # Process data.
    #
    # Drop duplicates (the row for Denmark is duplicated, where all columns have the exact same value, which is zero).
    tb = tb.drop_duplicates().reset_index(drop=True)

    # Select necessary columns and rename them.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Convert column into a percentage.
    tb["yield_gap_versus_nitrogen_pollution_effect"] *= 100

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add a year column, assuming the year is the one when the paper was published.
    tb["year"] = 2020

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
