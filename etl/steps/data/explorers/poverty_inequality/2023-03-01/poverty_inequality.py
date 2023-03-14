"""World Inequality Database explorer data step.

Loads the latest WID data from garden and stores a table (as a csv file).
It also includes the LIS and WID datasets combined for a comparison explorer.

"""

import pandas as pd
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:

    # Load WID explorer step
    ds_wid: Dataset = paths.load_dependency("world_inequality_database")
    tb_wid = ds_wid["world_inequality_database"]

    # Load LIS explorer step
    ds_lis: Dataset = paths.load_dependency("luxembourg_income_study")
    tb_lis = ds_lis["luxembourg_income_study"]

    # Merge both explorer datasets and assign a short name
    tb_explorer = pd.merge(tb_wid, tb_lis, on=["country", "year"], how="outer", validate="one_to_one")

    tb_explorer.metadata.short_name = "poverty_inequality"

    # Create explorer dataset with merged table in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_explorer], formats=["csv"])
    ds_explorer.save()
