"""Create the animal-welfare multidim, shown as a data explorer, with every view of the animal welfare multidims.

Its views are built in shared.py, which the smaller animal welfare multidims share.
"""

from shared import create_config

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Keep the estimate dropdown, so that the views of the published multidim keep their ids (see shared.py).
    c = paths.create_chart(config=create_config(paths, estimate_dimension=True))
    c.save()
