"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("mortality_database")
    tables = [ds_garden[tab] for tab in sorted(ds_garden.table_names)]

    ds_grapher = create_dataset(dest_dir=dest_dir, tables=tables, default_metadata=ds_garden.metadata)
    # Checks.
    #
    grapher_checks(ds_grapher, warn_title_public=False)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
