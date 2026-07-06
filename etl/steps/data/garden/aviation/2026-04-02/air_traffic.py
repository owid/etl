"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("air_traffic")

    # Read table from meadow dataset.
    tb = ds_meadow.read("air_traffic")

    #
    # Process data.
    #

    # Derive the share of empty seats from the passenger load factor.
    tb["share_empty_seats"] = 100 - tb["passenger_load_factor"]
    tb["share_empty_seats"].metadata.origins = tb["passenger_load_factor"].metadata.origins

    # The source publishes RPKs and ASKs in millions (columns "RPKs (mils)" and "ASKs (mils)").
    # Convert to actual passenger-kilometres and seat-kilometres.
    for col in ["revenue_passenger_kilometers", "available_seat_kilometers"]:
        tb[col] *= 1e6

    # Sanity check: after conversion, recent global traffic and capacity should be in the trillions.
    for col in ["revenue_passenger_kilometers", "available_seat_kilometers"]:
        assert 1e12 < tb[col].max() < 1e14, f"Unexpected magnitude for {col} — check the source units."

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
