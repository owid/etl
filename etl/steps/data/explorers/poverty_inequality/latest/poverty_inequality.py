"""

Loads the latest PIP, WID and LIS explorer steps and stores a table (as a csv file) to use for a comparison explorer.

"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP versions
# NOTE: Change this in case of new PPP versions in the future
PPP_YEAR_OLD = 2017
PPP_YEAR_CURRENT = 2021


def run() -> None:
    # Load WID explorer step.
    ds_wid = paths.load_dataset("world_inequality_database")
    tb_wid = ds_wid["world_inequality_database"].reset_index()

    # Load LIS explorer step
    ds_lis = paths.load_dataset("luxembourg_income_study")
    tb_lis = ds_lis["luxembourg_income_study"].reset_index()

    # Load PIP data
    ds_pip = paths.load_dataset("world_bank_pip")
    tb_pip_old = ds_pip[f"income_consumption_{PPP_YEAR_OLD}"].reset_index()
    tb_pip_current = ds_pip[f"income_consumption_{PPP_YEAR_CURRENT}"].reset_index()

    # Create explorer tables
    tb_explorer_old = merge_tables(
        tb_pip=tb_pip_old,
        tb_wid=tb_wid,
        tb_lis=tb_lis,
        short_name=f"poverty_inequality_{PPP_YEAR_OLD}",
    )
    tb_explorer_current = merge_tables(
        tb_pip=tb_pip_current,
        tb_wid=tb_wid,
        tb_lis=tb_lis,
        short_name=f"poverty_inequality_{PPP_YEAR_CURRENT}",
    )

    # Create explorer dataset with merged table in csv format
    ds_explorer = paths.create_dataset(tables=[tb_explorer_old, tb_explorer_current], formats=["csv"])
    ds_explorer.save()


def merge_tables(tb_pip: Table, tb_wid: Table, tb_lis: Table, short_name: str) -> Table:
    """
    Merge the tables from PIP, WID and LIS datasets.
    """
    # Merge explorer datasets and assign a short name
    tb_explorer = pr.merge(
        tb_wid,
        tb_lis,
        on=["country", "year"],
        how="outer",
        validate="one_to_one",
    )
    tb_explorer = pr.merge(
        tb_explorer,
        tb_pip,
        on=["country", "year"],
        how="outer",
        validate="one_to_one",
    )

    # Drop null rows in all columns except country and year
    tb_explorer = tb_explorer.dropna(
        how="all",
        subset=[x for x in tb_explorer.columns if x not in ["country", "year"]],
    )

    # Verify index and sort
    tb_explorer = tb_explorer.format(["country", "year"], short_name=short_name)

    return tb_explorer
