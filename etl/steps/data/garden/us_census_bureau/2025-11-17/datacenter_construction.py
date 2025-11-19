"""Garden step for datacenter construction spending data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("datacenter_construction")

    # Read table from meadow dataset.
    tb = ds_meadow.read("datacenter_construction")

    #
    # Process data.
    #

    # Add country column (this is U.S. data)
    tb["country"] = "United States"

    # Convert spending from millions to actual dollars for consistency
    tb["datacenter_construction_spending"] = tb["datacenter_construction_spending"] * 1_000_000

    # Set appropriate format and metadata
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
