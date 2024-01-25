"""

Loads the latest PIP, WID and LIS explorer steps and stores a table (as a csv file) to use for a comparison explorer.

"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load WID explorer step
    ds_wid = paths.load_dataset("world_inequality_database")
    tb_wid = ds_wid["world_inequality_database"].reset_index()

    # Load LIS explorer step
    ds_lis = paths.load_dataset("luxembourg_income_study")
    tb_lis = ds_lis["luxembourg_income_study"].reset_index()

    # Load PIP data
    ds_pip = paths.load_dataset("world_bank_pip")
    tb_pip = ds_pip["income_consumption_2017"].reset_index()

    # Merge explorer datasets and assign a short name
    tb_explorer = pr.merge(
        tb_wid, tb_lis, on=["country", "year"], how="outer", validate="one_to_one", short_name="poverty_inequality"
    )
    tb_explorer = pr.merge(
        tb_explorer, tb_pip, on=["country", "year"], how="outer", validate="one_to_one", short_name="poverty_inequality"
    )

    # Verify index and sort
    tb_explorer = tb_explorer.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create explorer dataset with merged table in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_explorer], formats=["csv"])
    ds_explorer.save()
