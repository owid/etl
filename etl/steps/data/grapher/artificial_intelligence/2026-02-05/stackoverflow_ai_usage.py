"""Load Stack Overflow AI usage garden dataset into grapher."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create grapher dataset."""
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("stackoverflow_ai_usage")
    tb = ds_garden.read("stackoverflow_ai_usage", reset_index=False)

    #
    # Save outputs.
    #
    # Grapher expects a "country" dimension — map "response" to it so each
    # response category becomes a separate line/bar in the chart.
    tb = tb.rename_index_names({"response": "country"})

    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()
