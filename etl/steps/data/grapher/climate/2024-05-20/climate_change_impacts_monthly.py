"""Load a garden dataset and create a grapher dataset.

"""

from etl.grapher_helpers import adapt_table_with_dates_to_grapher
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its monthly table.
    ds_garden = paths.load_dataset("climate_change_impacts")
    tb = ds_garden["climate_change_impacts_monthly"].reset_index()

    #
    # Process data.
    #
    # Create a country column (required by grapher).
    tb = tb.rename(columns={"location": "country"}, errors="raise")

    # Adapt table with dates to grapher requirements.
    tb = adapt_table_with_dates_to_grapher(tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
