# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    # ds = paths.load_dataset("ucdp_prio")
    # tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # Create mdim
    c = paths.create_collection(
        config=config,
        short_name="migration-flows",
    )

    # Save & upload
    c.save()
