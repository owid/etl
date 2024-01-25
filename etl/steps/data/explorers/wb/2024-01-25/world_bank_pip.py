"""World Bank PIP explorer data step.

Loads the latest PIP data from garden and stores multiple tables as csv diles.

"""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load garden dataset.
    ds_garden = paths.load_dataset("world_bank_pip")

    # Read table from garden dataset.
    tb_inc_or_cons_2017 = ds_garden["income_consumption_2017"]

    # Drop welfare variables not used in the explorers
    drop_list = ["above", "poverty_severity", "watts", "stacked", "headcount_215_regions", "surveys_past_decade"]

    for var in drop_list:
        tb_inc_or_cons_2017 = tb_inc_or_cons_2017[
            tb_inc_or_cons_2017.columns.drop(list(tb_inc_or_cons_2017.filter(like=var)))
        ]

    # Remove rows with missing values (except for country and year) to make the table smaller
    tb_inc_or_cons_2017 = tb_inc_or_cons_2017.dropna(
        how="all", subset=[x for x in tb_inc_or_cons_2017.columns if x not in ["country", "year"]]
    )

    # Import the rest of the tables
    rest_of_tables = []
    for table in [
        t
        for t in ds_garden.table_names
        if t
        not in [
            "income_consumption_2017",
            "income_consumption_2011",
            "income_2011",
            "consumption_2011",
        ]
    ]:
        tb = ds_garden[table]
        rest_of_tables.append(tb)

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(
        dest_dir, tables=[tb_inc_or_cons_2017] + rest_of_tables, default_metadata=ds_garden.metadata, formats=["csv"]
    )
    ds_explorer.save()
