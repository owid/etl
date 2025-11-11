"""Garden step for NVIDIA quarterly revenue data."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    # Mapping of old values to 'niner ones'
    rename_map = {
        "Professional Visualization": "Professional visualization",
        "Gaming": "Gaming",
        "TOTAL": "Total",
        "Auto": "Auto",
        "OEM & Other": "Original equipment manufacturing and other",
        "Data Center": "Data centers",
    }

    # Replace values in the column
    tb["segment"] = tb["segment"].replace(rename_map)

    # Set appropriate format and metadata
    tb = tb.format(["date", "segment"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
