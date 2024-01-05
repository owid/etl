from owid.catalog import Dataset

from etl.helpers import create_dataset, grapher_checks


def run_wrapper(dest_dir: str, garden_dataset: Dataset) -> None:
    # Read tables from garden dataset.
    tables = [garden_dataset[table_name] for table_name in garden_dataset.table_names]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=garden_dataset.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
