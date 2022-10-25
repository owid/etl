import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo

from etl.helpers import Names

# Get naming conventions.
N = Names(__file__)

# Mapping of names for technologies (as well as solar sub-technologies).
TECHNOLOGIES_RENAMING = {
    "Bioenergy": "bioenergy",
    "Enabling Technologies": "other",
    "Geothermal Energy": "geothermal",
    "Hydropower": "hydropower",
    "Ocean Energy": "marine",
    "PV": "solar_photovoltaic",
    "Solar Thermal": "solar_thermal",
    "PV - Thermal Hybrid": "solar_photovoltaic_and_thermal_hybrid",
    "Wind Energy": "wind",
}


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

    # Include solar sub-technologies as technologies.
    solar_mask = df["technology"] == "Solar Energy"
    df = df.astype({"technology": str})
    df.loc[solar_mask, "technology"] = df[solar_mask]["sub_technology"]

    # Simplify table to keep only number of patents for each technology (ignoring sector and sub_technology).
    df = df.groupby(["country", "year", "technology"]).agg({"patents": "sum"}).reset_index()

    # Create a dataframe of global count of patents.
    global_patents = df.groupby(["year", "technology"]).agg({"patents": "sum"}).reset_index()
    global_patents["country"] = "World"

    # Add global count of patents to original dataframe.
    df = pd.concat([df, global_patents], ignore_index=True)

    # Change from long to wide format dataframe.
    df = df.pivot(index=["country", "year"], columns="technology", values="patents").reset_index()

    # Remove name of dummy index.
    df.columns.names = [None]

    # Rename columns conveniently.
    df = df.rename(columns=TECHNOLOGIES_RENAMING, errors="raise")

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

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
