import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo

from etl.helpers import Names

# Get naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow.
    ds_meadow = N.meadow_dataset
    # Load main table from dataset.
    tb_meadow = ds_meadow[ds_meadow.table_names[0]]
    # Create a dataframe from table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    df = geo.harmonize_countries(df=df, countries_file=N.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year", "sector", "technology", "sub_technology"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Initialize a new Garden dataset, using metadata from Meadow.
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    # Ensure all columns are snake, lower case.
    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    # Update dataset metadata using the metadata yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    # Update table metadata using the metadata yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, ds_meadow.table_names[0])
    # Add table to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()
