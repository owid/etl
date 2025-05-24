"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_weapons_inventories")
    tb = ds_meadow.read("nuclear_weapons_inventories")

    #
    # Process data.
    #
    # Fix formatting of column with number of warheads (which contains thousand commas).
    # NOTE: Looking at the original dashboards, it seems that missing values are shown as zeros.
    tb["number_of_warheads"] = tb["number_of_warheads"].str.replace(",", "").astype(float).fillna(0).astype("Int64")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
