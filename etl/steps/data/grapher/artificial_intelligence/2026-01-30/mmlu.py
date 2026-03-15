"""Load MMLU benchmark dataset into grapher."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load MMLU dataset into grapher."""
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("mmlu")
    tb = ds_garden.read("mmlu", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()
