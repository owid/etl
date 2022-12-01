from copy import deepcopy

from owid import catalog

from etl.paths import DATA_DIR

# Define inputs.
GARDEN_VERSION = "2022-11-24"
GARDEN_DATASET_PATH = DATA_DIR / f"garden/emdat/{GARDEN_VERSION}/natural_disasters"
GARDEN_TABLE_NAME = "natural_disasters_yearly"
# Define outputs.
GRAPHER_DATASET_NAME = GARDEN_TABLE_NAME

DISASTER_TYPE_RENAMING = {
    'all_disasters': 'All disasters',
    'animal_accident': 'Animal accident',
    'drought': 'Drought',
    'earthquake': 'Earthquake',
    'epidemic': 'Epidemic',
    'extreme_temperature': 'Extreme temperature',
    'flood': 'Flood',
    'fog': 'Fog',
    'glacial_lake_outburst': 'Glacial lake outburst',
    'impact': 'Impact',
    'insect_infestation': 'Insect infestation',
    'landslide': 'Landslide',
    'mass_movement__dry': 'Dry mass movement',
    'storm': 'Storm',
    'volcanic_activity': 'Volcanic activity',
    'wildfire': 'Wildfire',
}


def run(dest_dir: str) -> None:
    # Load garden dataset.
    garden_dataset = catalog.Dataset(GARDEN_DATASET_PATH)

    # Load yearly table from garden dataset.
    table = garden_dataset[GARDEN_TABLE_NAME]

    # Create wide dataframes.
    table_wide = table.reset_index().pivot(index=["country", "year"], columns="type")

    # Store metadata of original table variables.
    variable_metadata = {}
    for column, subcolumn in table_wide.columns:
        old_metadata = deepcopy(table[column].metadata)
        old_metadata.title = f"{old_metadata.title} - {DISASTER_TYPE_RENAMING[subcolumn]}"
        variable_metadata[f"{column}_{subcolumn}"] = old_metadata

    # Flatten column indexes.
    table_wide.columns = [f"{column}_{subcolumn}" for column, subcolumn in table_wide.columns]

    # Assign original variables metadata to new variables in wide table.
    for variable in variable_metadata:
        table_wide[variable].metadata = variable_metadata[variable]

    # Create grapher dataset, add table, and save dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.add(table_wide)
    dataset.save()
