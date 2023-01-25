import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import dataframes
from shared import CURRENT_DIR

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get naming conventions.
N = PathFinder(str(CURRENT_DIR / "renewable_energy_patents"))

SUB_TECHNOLOGY_RENAMING = {
    "Smart Grids": "Smart grids",
    "Thermal energy storage": "Thermal energy storage",
    "Hydrogen (storage and distribution and applications)": "Hydrogen",
    "Hydropower": "Hydropower",
    "PV": "Solar PV",
    "Solar Thermal": "Solar thermal",
    "CCUS": "Carbon capture and storage",
    "Cross-cutting": "Cross-cutting energy tech",
    "Energy Storage - General": "Storage (general)",
    "Biofuels": "Bioenergy",
    "Fuel from waste": "Bioenergy",
    "Batteries": "Batteries",
    "Fuel Cells": "Fuel cells",
    "Green hydrogen (water eloctrolysis)": "Green hydrogen",
    "Ocean Energy": "Marine and tidal",
    "PV - Thermal Hybrid": "Solar PV-thermal hybrid",
    "Wind Energy": "Wind",
    "Electromobility - Charging Stations": "EV charging stations",
    "Electromobility - Energy Storage": "EV storage",
    "Energy Efficiency": "Efficiency",
    "Others": "Other",
    "Ultracapacitors, supercapacitors, double-layer capacitors": "Ultracapacitors",
    "Mechanical energy storage, e.g. flywheels or pressurised fluids": "Storage (excl. batteries)",
    "Geothermal Energy": "Geothermal",
    "Electromobility - Electric Energy Management": "EV management",
    "Electromobility - Information/Communication Technologies": "EV communication tech",
    "Electromobility - Machine related technology ": "EV machine tech",
    "Heat pumps": "Geothermal",
}

# List of aggregate regions to create.
REGIONS_TO_ADD = [
    "World",
    # Continents.
    "Africa",
    "Asia",
    "Europe",
    "European Union (27)",
    "North America",
    "Oceania",
    "South America",
    # Income groups.
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
]


def regroup_sub_technologies(df: pd.DataFrame) -> pd.DataFrame:
    """Rename sub-technologies conveniently, ignore sector and technology, and recalculate the number of patents per
    sub-technology.

    Parameters
    ----------
    df : pd.DataFrame
        Data with number of patents per country, year, sector, technology and sub-technology.

    Returns
    -------
    df : pd.DataFrame
        Data with number of patents per country, year and sub-technology.

    """
    # It seems that sub-technologies can belong to only one technology.
    # As long as this is true, we can just ignore "technology" and select by "sub_technology".
    assert set(df.groupby("sub_technology").agg({"technology": "nunique"}).reset_index()["technology"]) == {1}
    # This does not happen with sectors.
    # For example, sub-technology Biofuels is included in sectors "Power", "Transport", and "Industry".

    # There seems to be one nan sub-technology. Add this to "Others".
    df["sub_technology"] = df["sub_technology"].fillna("Others")

    # Rename sub-technologies conveniently.
    df["sub_technology"] = dataframes.map_series(
        series=df["sub_technology"],
        mapping=SUB_TECHNOLOGY_RENAMING,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )

    # After renaming, some sub-technologies have been combined (e.g. Biofuels and Fuel from waste are now both Bioenergy).
    # Re-calculate numbers of patents with the new sub-technology groupings.
    df = (
        df.groupby(["country", "year", "sector", "technology", "sub_technology"], observed=True)
        .agg({"patents": "sum"})
        .reset_index()
    )

    return df


def add_region_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Add number of patents for regions (the world, continents and income groups) by summing the contribution of the
    region's members.

    Parameters
    ----------
    df : pd.DataFrame
        Data with a dummy index, country, year, sector, technology, sub-technology and patents.

    Returns
    -------
    df : pd.DataFrame
        Data after adding region aggregates.

    """
    # Add aggregates for continents and income groups.
    for region in REGIONS_TO_ADD:
        if region == "World":
            # For the world, add all countries that are not in regions (also exclude any regions given in the original
            # data, that should be named "* (IRENA)" after harmonization, if there was any).
            countries_in_region = [
                country
                for country in df["country"].unique()
                if country not in REGIONS_TO_ADD
                if not country.endswith("(IRENA)")
            ]
        else:
            # List countries in region.
            countries_in_region = geo.list_countries_in_region(region=region)
        region_data = (
            df[df["country"].isin(countries_in_region)]
            .groupby(["year", "sector", "technology", "sub_technology"], observed=True)
            .agg({"patents": "sum"})
            .reset_index()
            .assign(**{"country": region})
        )
        # Add data for new region to dataframe.
        df = pd.concat([df, region_data], ignore_index=True)

    return df


def create_patents_by_sub_technology(df: pd.DataFrame) -> pd.DataFrame:
    """Create a simplified wide dataframe that counts number of patents by sub-technologies.

    It will also add a column for the total number of patents (the sum of patents of all sub-technologies).

    Parameters
    ----------
    df : pd.DataFrame
        Data in long dataframe format (indexed by country, year, sector, technology and sub-technology).

    Returns
    -------
    patents_by_sub_technology : pd.DataFrame
        Wide dataframe of number of patents, indexed by country and year, and with a column per sub-technology.

    """
    # Create a simplified table giving just number of patents by sub-technology.
    # Re-calculate numbers of patents by sub-technology.
    patents_by_sub_technology = (
        df.reset_index()
        .groupby(["country", "year", "sub_technology"], observed=True)
        .agg({"patents": "sum"})
        .reset_index()
    )

    # Restructure dataframe to have a column per sub-technology.
    patents_by_sub_technology = patents_by_sub_technology.pivot(
        index=["country", "year"], columns="sub_technology", values="patents"
    ).reset_index()
    # Remove name of dummy index.
    patents_by_sub_technology.columns.names = [None]

    # Set an appropriate index to the table by sub-technology and sort conveniently.
    patents_by_sub_technology = patents_by_sub_technology.set_index(
        ["country", "year"], verify_integrity=True
    ).sort_index()

    # Create a column for the total number of patents of all sub-technologies.
    patents_by_sub_technology["Total patents"] = patents_by_sub_technology.sum(axis=1)

    return patents_by_sub_technology


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
    # Rename sub-technologies conveniently, and regroup them according to the new sub-technology names.
    df = regroup_sub_technologies(df=df)

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=N.country_mapping_path)

    # Add region aggregates.
    df = add_region_aggregates(df=df)

    # Set an appropriate index to main table and sort conveniently.
    df = df.set_index(["country", "year", "sector", "technology", "sub_technology"], verify_integrity=True).sort_index()

    # Create a new, simplified dataframe, that shows number of patents by sub-technology.
    patents_by_sub_technology = create_patents_by_sub_technology(df=df)

    #
    # Save outputs.
    #
    # Initialize a new Garden dataset, using metadata from Meadow.
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    # Ensure all columns are snake, lower case.
    tb_garden = underscore_table(Table(df))
    tb_garden_by_sub_technology = underscore_table(Table(patents_by_sub_technology))
    tb_garden.metadata = tb_meadow.metadata
    tb_garden_by_sub_technology.metadata = tb_meadow.metadata

    # Update dataset metadata using the metadata yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")

    # Update tables metadata using the metadata yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, "renewable_energy_patents")
    tb_garden_by_sub_technology.update_metadata_from_yaml(N.metadata_path, "renewable_energy_patents_by_technology")

    # Add tables to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.add(tb_garden_by_sub_technology)
    ds_garden.save()
