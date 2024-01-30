"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("mass_balance_us_glaciers")
    tb = ds_meadow["mass_balance_us_glaciers"].reset_index()

    #
    # Process data.
    #
    # Add a location column.
    tb["location"] = "United States"

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["location", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
