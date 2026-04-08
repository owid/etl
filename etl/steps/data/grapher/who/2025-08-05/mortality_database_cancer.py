"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("mortality_database_cancer")
    tables = list(ds_garden)

    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    # Checks.
    #
    grapher_checks(ds_grapher, warn_title_public=False)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
