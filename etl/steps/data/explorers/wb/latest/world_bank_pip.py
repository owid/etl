"""
World Bank PIP explorer data step.

Loads the latest PIP data from garden and stores multiple tables as csv files.

"""

from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP versions
# NOTE: Change this in case of new PPP versions in the future
PPP_VERSIONS = [2017, 2021]

# Define International Poverty Line (in cents)
# NOTE: Change this in case of new IPL in the future
# TODO: Change to 2021 prices
INTERNATIONAL_POVERTY_LINE = 215


def run(dest_dir: str) -> None:
    # Load garden dataset.
    ds_garden = paths.load_dataset("world_bank_pip")

    # Read table from garden dataset.
    tb_inc_or_cons = ds_garden[f"income_consumption_{PPP_VERSIONS[1]}"]

    # Drop variables not used in the explorers and rows with missing values
    tb_inc_or_cons = drop_columns_and_rows(
        tb=tb_inc_or_cons,
        drop_list=[
            "above",
            "between",
            "poverty_severity",
            "watts",
            f"headcount_{INTERNATIONAL_POVERTY_LINE}_regions",
            "surveys_past_decade",
            "reporting_level",
            "welfare_type",
            "region_name",
        ],
    )

    # Create a separate table for PIP inequality data
    tb_pip_inequality = create_inequality_table(tb=tb_inc_or_cons, short_name="pip_inequality")

    # Import the rest of the tables
    rest_of_tables = import_rest_of_tables(ds_garden=ds_garden)

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(
        dest_dir,
        tables=[tb_inc_or_cons, tb_pip_inequality] + rest_of_tables,
        default_metadata=ds_garden.metadata,
        formats=["csv"],
    )
    ds_explorer.save()


def drop_columns_and_rows(tb: Table, drop_list: list) -> Table:
    """
    Drop columns and rows with missing values
    """

    for var in drop_list:
        tb = tb[tb.columns.drop(list(tb.filter(like=var)))]

    # Remove rows with missing values (except for country and year) to make the table smaller
    tb = tb.dropna(how="all", subset=[x for x in tb.columns if x not in ["country", "year"]])

    return tb


def import_rest_of_tables(ds_garden: Dataset) -> list:
    """
    Import the rest of the tables, iterating over the garden dataset
    """

    rest_of_tables = []
    for table in [
        t
        for t in ds_garden.table_names
        if t
        not in [
            f"income_consumption_{PPP_VERSIONS[1]}",
            f"income_consumption_{PPP_VERSIONS[0]}",
            f"income_{PPP_VERSIONS[0]}",
            f"consumption_{PPP_VERSIONS[0]}",
            f"percentiles_income_consumption_{PPP_VERSIONS[0]}",
            f"percentiles_income_consumption_{PPP_VERSIONS[1]}",
        ]
    ]:
        tb = ds_garden[table]
        tb = drop_columns_and_rows(
            tb=tb, drop_list=["above", "between", "poverty_severity", "watts", "reporting_level", "welfare_type"]
        )
        rest_of_tables.append(tb)

    return rest_of_tables


def create_inequality_table(tb: Table, short_name: str) -> Table:
    """
    Create a table with only PIP inequality data and removing regions (without inequality indicators)
    """

    tb_pip_inequality = tb.reset_index()

    # Define list of variables
    inequality_vars = [
        "country",
        "year",
        "gini",
        "decile10_share",
        "bottom50_share",
        "palma_ratio",
        "headcount_ratio_50_median",
    ]

    tb_pip_inequality = tb_pip_inequality[inequality_vars]

    # Remove regions, because they don't have inequality data
    tb_pip_inequality = tb_pip_inequality[~tb_pip_inequality["country"].str.contains("\\(PIP\\)")]

    # Add short name
    tb_pip_inequality.metadata.short_name = short_name

    # Verify index and sort
    tb_pip_inequality = tb_pip_inequality.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb_pip_inequality
