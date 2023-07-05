from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Details for output dataset.
DATASET_TITLE = "Renewable electricity capacity by technology (IRENA, 2023)"


def run(dest_dir: str) -> None:
    # Load dataset from Garden.
    ds_garden: catalog.Dataset = paths.load_dependency("renewable_electricity_capacity")
    # Load main table from dataset.
    table = ds_garden["renewable_electricity_capacity"]

    # Get the human-readable names of the technologies from the variable metadata.
    rename_technologies = {variable: table[variable].metadata.title for variable in table.columns}

    # Simplify table to consider only the World.
    # Here we use "country" to refer to a technology.
    # This is a workaround, so that grapher will let us select technologies as it does with countries.
    table = table.loc["World"].reset_index().melt(id_vars="year", var_name="country", value_name="capacity")

    # Rename technologies conveniently.
    table = table.replace(rename_technologies)

    # Set appropriate metadata.
    table["country"].metadata.unit = None
    table["country"].metadata.short_unit = None
    table["capacity"].metadata.title = "Capacity"
    table["capacity"].metadata.unit = "megawatts"
    table["capacity"].metadata.short_unit = "MW"
    table["capacity"].metadata.display = {"numDecimalPlaces": 0}

    # Create new dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)
    dataset.metadata.title = DATASET_TITLE
    dataset.metadata.short_name = paths.short_name

    dataset.add(table)
    dataset.save()
