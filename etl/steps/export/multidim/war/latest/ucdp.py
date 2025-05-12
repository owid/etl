# from etl.collection import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    # ds = paths.load_dataset("ucdp")
    # tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # Create mdim
    mdim = paths.create_collection(
        config=config,
        short_name="migration-flows",
    )

    # Save & upload
    mdim.save()
