"""List the datasets used in the technical publication "How many chickens are killed in egg production?".

The publication is the notebook docs/analyses/chickens_killed_in_egg_production/chickens_killed_in_egg_production.ipynb.
Some of the datasets it loads (from USDA, Defra and Eurostat) are not used in any chart. Listing them as dependencies of
this step in the dag shows where they are used, so they are not flagged as unused.

This step processes no data, and saves an empty dataset.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Save outputs.
    #
    # Create an empty dataset.
    ds = paths.create_dataset(tables=[])

    # Save the empty dataset.
    ds.save()
