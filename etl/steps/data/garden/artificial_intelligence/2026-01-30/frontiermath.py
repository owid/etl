"""Process FrontierMath benchmark data for garden step."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Process FrontierMath benchmark data."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("frontiermath")

    # Read table from meadow dataset.
    tb = ds_meadow.read("epoch_benchmark_data")

    #
    # Process data.
    #
    tb["mean_score"] = tb["mean_score"] * 100

    tb = tb.format(["release_date", "model_version"])
    #
    # Save outputs.
    #
    # Create garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes.
    ds_garden.save()
