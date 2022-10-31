from owid import catalog
from shared import GARDEN_VERSION, VERSION

from etl.paths import DATA_DIR, STEP_DIR

# Details of input garden dataset and table.
GARDEN_DATASET_NAME = "world_carbon_pricing"
GARDEN_DATASET_PATH = DATA_DIR / "garden" / "rff" / GARDEN_VERSION / GARDEN_DATASET_NAME
GARDEN_TABLE_NAME = "world_carbon_pricing_any_sector"
# Details of output grapher dataset and table.
GRAPHER_DATASET_TITLE = "World carbon pricing for any sector (2022)"
GRAPHER_DATASET_NAME = GARDEN_TABLE_NAME
GRAPHER_METADATA_PATH = STEP_DIR / "grapher" / "rff" / VERSION / f"{GRAPHER_DATASET_NAME}.meta.yml"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_garden = catalog.Dataset(GARDEN_DATASET_PATH)
    # Get table from dataset.
    tb_garden = ds_garden[GARDEN_TABLE_NAME]

    #
    # Save outputs.
    #
    # Prepare metadata for new grapher dataset.
    grapher_metadata = ds_garden.metadata
    grapher_metadata.title = GRAPHER_DATASET_TITLE
    # Create new grapher dataset.
    ds_grapher = catalog.Dataset.create_empty(dest_dir, grapher_metadata)
    # Add table to new dataset.
    ds_grapher.add(tb_garden)
    # Save dataset.
    ds_grapher.save()
