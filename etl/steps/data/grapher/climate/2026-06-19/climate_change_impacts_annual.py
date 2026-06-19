"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its annual table.
    ds_garden = paths.load_dataset("climate_change_impacts")
    tb_annual = ds_garden.read("climate_change_impacts_annual")

    #
    # Process data.
    #
    # Create a country column (required by grapher).
    tb_annual = tb_annual.rename(columns={"location": "country"}, errors="raise")

    # Set an appropriate index and sort conveniently.
    tb_annual = tb_annual.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_annual])
    ds_grapher.save()
