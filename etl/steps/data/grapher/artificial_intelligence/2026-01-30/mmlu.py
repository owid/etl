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

    tb_models = ds_garden.read("mmlu_by_model", reset_index=False)
    tb_country = ds_garden.read("mmlu_by_country", reset_index=False)

    # Rename release_date index to date for grapher.
    tb_models = tb_models.rename_index_names({"release_date": "date"})
    tb_country = tb_country.rename_index_names({"release_date": "date"})

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(
        tables=[tb_models, tb_country], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
