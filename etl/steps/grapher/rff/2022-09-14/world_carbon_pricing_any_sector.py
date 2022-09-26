import pandas as pd
from owid import catalog
from owid.catalog import Table
from owid.catalog.utils import underscore_table

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR, STEP_DIR

# Details of input garden dataset and table.
GARDEN_DATASET_NAME = "world_carbon_pricing"
GARDEN_DATASET_PATH = DATA_DIR / "garden" / "rff" / "2022-09-14" / GARDEN_DATASET_NAME
GARDEN_TABLE_NAME = GARDEN_DATASET_NAME
# Details of output grapher dataset and table.
GRAPHER_DATASET_TITLE = "World carbon pricing for any sector"
GRAPHER_DATASET_NAME = f"{GARDEN_TABLE_NAME}_any_sector"
GRAPHER_METADATA_PATH = STEP_DIR / "grapher" / "rff" / "2022-09-14" / f"{GRAPHER_DATASET_NAME}.meta.yml"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_garden = catalog.Dataset(GARDEN_DATASET_PATH)
    # Get table from dataset.
    tb_garden = ds_garden[GARDEN_TABLE_NAME]
    # Construct a dataframe from the table.
    df = pd.DataFrame(tb_garden)

    #
    # Process data.
    #
    # Create a simplified table that simply gives, for each country and year, whether the country has any sector(-fuel)
    # that is covered by at least one tax instrument. And idem for ets.
    df_any_sector = (
        df.reset_index()
        .groupby(["country", "year"], observed=True)
        .agg({"ets": lambda x: x.sum() > 0, "tax": lambda x: x.sum() > 0})
        .astype(int)
        .reset_index()
    )
    # Set an appropriate index and sort conveniently.
    df_any_sector = df_any_sector.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    # Create table for simplified data.
    tb_grapher_any_sector = underscore_table(Table(df_any_sector)).reset_index()

    #
    # Save outputs.
    #
    # Prepare metadata for new grapher dataset.
    # Start with the garden metadata, and updated it using the grapher metadata yaml file.
    grapher_metadata = ds_garden.metadata
    grapher_metadata.update_from_yaml(GRAPHER_METADATA_PATH, if_source_exists="append")
    # Create new grapher dataset.
    ds_grapher = catalog.Dataset.create_empty(dest_dir, grapher_metadata)
    # Prepare metadata for new grapher table.
    # Start with the garden metadata and update it using the grapher metadata yaml file.
    tb_grapher_any_sector.metadata = tb_garden.metadata
    tb_grapher_any_sector.update_metadata_from_yaml(GRAPHER_METADATA_PATH, GRAPHER_DATASET_NAME)
    # Add table to new dataset.
    ds_grapher.add(tb_grapher_any_sector)
    # Save dataset.
    ds_grapher.save()
