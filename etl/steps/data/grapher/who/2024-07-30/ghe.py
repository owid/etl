# NOTE: This is a massive dataset with 50M rows and 50k variables (there are just 4 actual
# columns, but 12500 dimension combinations). It takes ~1.5h to upload it to grapher with
# 40 workers.

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load garden dataset
    ds_garden = paths.load_dataset("ghe")

    # Read table from garden dataset.
    tb_garden = ds_garden["ghe"]
    tb_garden_ratio = ds_garden["ghe_suicides_ratio"]

    #
    # Save outputs.
    #
    tables = [
        tb_garden_ratio,
        tb_garden,
    ]

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=ds_garden.metadata, repack=False)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
