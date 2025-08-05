"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("literacy_1451_1800")

    # Read table from meadow dataset.
    tb = ds_meadow.read("literacy_1451_1800")

    #
    # Process data.
    #

    # Convert year ranges to middle points
    tb["year"] = tb["year"].apply(convert_year_to_midpoint)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def convert_year_to_midpoint(year_str):
    """Convert year ranges like '1451-1500' to their middle point (1475)."""
    if "-" in str(year_str):
        start, end = map(int, str(year_str).split("-"))
        return (start + end) // 2
    else:
        return int(year_str)
