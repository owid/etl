"""Load garden dataset and create a grapher dataset for WHO Mortality Database (aggregated)."""

from etl.helpers import PathFinder, grapher_checks

paths = PathFinder(__file__)


def run() -> None:
    """
    Load garden dataset and create grapher dataset.
    """
    # Load garden dataset
    ds_garden = paths.load_dataset("mortality_database_aggregated")

    # Get all tables
    tables = [ds_garden[tab] for tab in sorted(ds_garden.table_names)]

    # Create grapher dataset
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)

    # Run checks
    grapher_checks(ds_grapher, warn_title_public=False)

    # Save
    ds_grapher.save()
