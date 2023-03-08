"""World Inequality Database explorer data step.

Loads the latest WID data from garden and stores a table (as a csv file).

"""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:

    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("world_inequality_database")

    # Read table from garden dataset.
    tb_garden = ds_garden["world_inequality_database"]

    # Drop welfare variables not used in the explorers
    drop_list = ["posttax_dis"]

    for var in drop_list:
        tb_garden = tb_garden[tb_garden.columns.drop(list(tb_garden.filter(like=var)))]

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata, formats=["csv"])
    ds_explorer.save()
