from etl.collection import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=False)

    # Create and save collection
    c = paths.create_collection(
        config=config,
        short_name="mdd-vaccination-who",
        tb=tb,
        indicator_names=["coverage", "unvaccinated", "vaccinated"],
        dimensions=["antigen"],
        indicators_slug="metric",
    )
    c.save()
