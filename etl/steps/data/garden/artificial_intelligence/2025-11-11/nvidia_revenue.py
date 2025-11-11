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
    tb = ds_meadow["nvidia_revenue"].reset_index()

    #
    # Process data.
    #
    # Add a 'country' column for NVIDIA (worldwide data)
    tb["country"] = "World"

    # Calculate year-over-year growth for each segment
    tb = tb.sort_values(["segment", "date"])
    tb["revenue_yoy_growth"] = tb.groupby("segment")["revenue_millions"].pct_change(periods=4) * 100

    # Calculate quarter-over-quarter growth
    tb["revenue_qoq_growth"] = tb.groupby("segment")["revenue_millions"].pct_change() * 100

    # Set appropriate format and metadata
    tb = tb.format(["country", "date", "segment"], short_name="nvidia_revenue")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
