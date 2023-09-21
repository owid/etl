from owid import catalog

from etl.paths import DATA_DIR

# Details for output dataset.
DATASET_NAME = "renewable_electricity_capacity_by_technology"
DATASET_TITLE = "Renewable electricity capacity by technology"
# Details for input dataset.
GARDEN_DATASET_PATH = DATA_DIR / "garden/irena/2022-10-20/renewable_electricity_capacity"


def run(dest_dir: str) -> None:
    # Load dataset from Garden.
    garden_dataset = catalog.Dataset(GARDEN_DATASET_PATH)
    # Load main table from dataset.
    table = garden_dataset[garden_dataset.table_names[0]]

    # Get the human-readable names of the technologies from the variable metadata.
    rename_technologies = {variable: table[variable].metadata.title for variable in table.columns}

    # Simplify table to consider only the World.
    # Here we use "country" to refer to a technology.
    # This is a workaround, so that grapher will let us select technologies as it does with countries.
    table = table.loc["World"].reset_index().melt(id_vars="year", var_name="country", value_name="capacity")

    # Rename technologies conveniently.
    table = table.replace(rename_technologies)

    # Set appropriate metadata.
    table["capacity"].metadata.title = "Capacity"
    table["capacity"].metadata.unit = "Megawatts"
    table["capacity"].metadata.short_unit = "MW"
    table["capacity"].metadata.display = {"numDecimalPlaces": 0}

    # Create new dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.metadata.title = DATASET_TITLE
    dataset.metadata.short_name = DATASET_NAME

    dataset.add(table)
    dataset.save()
