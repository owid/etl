from owid import catalog

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load dataset from Garden and read its main table.
    ds_garden: catalog.Dataset = paths.load_dependency("renewable_electricity_capacity")
    tb = ds_garden["renewable_electricity_capacity"]

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], default_metadata=None)
    ds_grapher.metadata.title = f"Renewable electricity capacity (IRENA, {paths.version})"
    # Gather all sources in variables and assign them to the dataset.
    ds_grapher.metadata.sources = catalog.tables.get_unique_sources_from_tables([tb])
    ds_grapher.save()
