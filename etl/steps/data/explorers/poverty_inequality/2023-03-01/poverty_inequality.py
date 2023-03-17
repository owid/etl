"""

Loads the latest WID and LIS data from garden and stores a table (as a csv file) to use for a comparison explorer.
It also loads World Bank Poverty and Inequality Platform (PIP) data, currently outside the ETL (notebooks repo).
This data will be replaced in May 2023 by a PIP step inside the ETL

"""

import pandas as pd
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get path of PIP csv file
PIP_PATH = "https://raw.githubusercontent.com/owid/notebooks/main/BetterDataDocs/JoeHasell/PIP/data/ppp_2017/final/OWID_internal_upload/explorer_database/inc_or_cons/poverty_inc_or_cons.csv"

# Function to load and clean PIP data (will dissapear when PIP steps are created)
def add_pip_data(PIP_PATH: str):
    # Load PIP data
    tb_pip = pd.read_csv(PIP_PATH)

    # Rename country and year vars
    rename_list = {"Entity": "country", "Year": "year"}
    tb_pip = tb_pip.rename(columns=rename_list)

    # Drop welfare variables not used in the explorers
    drop_list = ["above", "poverty_severity", "watts", "stacked"]

    for var in drop_list:
        tb_pip = tb_pip[tb_pip.columns.drop(list(tb_pip.filter(like=var)))]

    # Additional variables to drop from PIP
    drop_list = [
        "survey_year",
        "survey_comparability",
        "comparable_spell",
        "distribution_type",
        "estimation_type",
        "cpi",
        "ppp",
        "reporting_gdp",
        "reporting_pce",
        "mld",
        "polarization",
    ]

    tb_pip = tb_pip.drop(columns=drop_list)

    # Rename regional aggregations
    regions_dict = {
        "Sub-Saharan Africa": "Sub-Saharan Africa (PIP)",
        "Europe and Central Asia": "Europe and Central Asia (PIP)",
        "High income countries": "High income countries (PIP)",
        "Latin America and the Caribbean": "Latin America and the Caribbean (PIP)",
        "South Asia": "South Asia (PIP)",
        "Middle East and North Africa": "Middle East and North Africa (PIP)",
        "East Asia and Pacific": "East Asia and Pacific (PIP)",
    }

    tb_pip["country"] = tb_pip["country"].replace(regions_dict)

    # Calculate bottom 50% share
    tb_pip["bottom50_share"] = (
        tb_pip["decile1_share"]
        + tb_pip["decile2_share"]
        + tb_pip["decile3_share"]
        + tb_pip["decile4_share"]
        + tb_pip["decile5_share"]
    )

    return tb_pip


def run(dest_dir: str) -> None:

    # Load WID explorer step
    ds_wid: Dataset = paths.load_dependency("world_inequality_database")
    tb_wid = ds_wid["world_inequality_database"].reset_index()

    # Load LIS explorer step
    ds_lis: Dataset = paths.load_dependency("luxembourg_income_study")
    tb_lis = ds_lis["luxembourg_income_study"].reset_index()

    # Load PIP data
    tb_pip = add_pip_data(PIP_PATH)

    # Merge explorer datasets and assign a short name
    tb_explorer = pd.merge(tb_wid, tb_lis, on=["country", "year"], how="outer", validate="one_to_one")
    tb_explorer = pd.merge(
        tb_explorer,
        tb_pip,
        on=["country", "year"],
        how="outer",
        validate="one_to_one",
    )

    # Verify index and sort
    tb_explorer = tb_explorer.set_index(["country", "year"], verify_integrity=True).sort_index()

    tb_explorer.metadata.short_name = "poverty_inequality"

    # Create explorer dataset with merged table in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_explorer], formats=["csv"])
    ds_explorer.save()
