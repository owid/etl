import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get naming conventions.
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load dataset from Meadow.
    ds_meadow = N.meadow_dataset
    # Load main table from dataset.
    tb_meadow = ds_meadow[ds_meadow.table_names[0]]

    # Create a dataframe out of the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=N.country_mapping_path)

    #
    # Save outputs.
    #
    # Create a new Garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)

    # Ensure all columns are snake, lower case.
    tb_garden = underscore_table(Table(df))

    # Load metadata from yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="append")
    tb_garden.update_metadata_from_yaml(N.metadata_path, ds_meadow.table_names[0])

    # Add table to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()
