"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to use from the data, and how to rename them.
COLUMNS = {
    "year": "year",
    "measure_names": "country",
    "measure_values": "number_of_warheads",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_weapons_inventories")
    tb = ds_meadow["nuclear_weapons_inventories"].reset_index()

    #
    # Process data.
    #
    # Rename columns.
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # Looking at the original dashboards, it seems that missing values are shown as zeros.
    tb["number_of_warheads"] = tb["number_of_warheads"].fillna(0)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
