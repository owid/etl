"""Garden step for NVIDIA quarterly revenue data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("nvidia_revenue")

    # Read table from meadow dataset.
    tb = ds_meadow.read("nvidia_revenue")

    #
    # Process data.
    #
    # Add a 'country' column for NVIDIA (worldwide data)
    tb = tb.drop(columns=["quarter"])
    tb["revenue_millions"] = tb["revenue_millions"] * 1_000_000  # Convert millions to actual dollars

    # Group Professional Visualization, Auto, and OEM & Other into "Other"
    rename_map = {
        "Professional Visualization": "Other",
        "Gaming": "Gaming",
        "TOTAL": "Total",
        "Auto": "Other",
        "OEM & Other": "Other",
        "Data Center": "Data centers and AI",
    }

    # Replace values in the column
    tb["segment"] = tb["segment"].replace(rename_map)

    # Group by date and segment to combine the "Other" categories
    tb = tb.groupby(["date", "segment"], as_index=False, observed=True)["revenue_millions"].sum()

    # Set appropriate format and metadata
    tb = tb.format(["date", "segment"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
