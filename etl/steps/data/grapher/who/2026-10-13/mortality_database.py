"""Load the WHO Mortality Database garden dataset and create the grapher dataset."""

from etl.helpers import PathFinder, grapher_checks

paths = PathFinder(__file__)


def run() -> None:
    # Step 1: Load the fact table. The `causes` lookup table stays in the catalog; it is not charted.
    ds_garden = paths.load_dataset("mortality_database")
    tb = ds_garden.read("mortality_database", reset_index=False)

    # Step 2: Save.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    grapher_checks(ds_grapher, warn_title_public=False)
    ds_grapher.save()
