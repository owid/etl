"""Load FrontierMath dataset into grapher."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load FrontierMath dataset into grapher."""
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("frontiermath")
    tb = ds_garden.read("epoch_benchmark_data", reset_index=False)

    # Rename index names for plotting
    tb = tb.rename_index_names({"release_date": "date", "model_version": "country"})
    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()
