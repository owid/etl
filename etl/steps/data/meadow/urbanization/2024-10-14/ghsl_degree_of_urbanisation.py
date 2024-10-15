"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_degree_of_urbanisation.xlsx")

    # Load data from snapshot.
    tb_area = snap.read(sheet_name="AREA_km2_L1")
    tb_population = snap.read(sheet_name="POP_L1")
    #
    # Process data.
    #
    # Remove rows where all values are NaNs
    tb_area = tb_area.dropna(how="all")
    tb_population = tb_population.dropna(how="all")

    columns_to_drop = ["IDs", "GADM_ISO", "sdg_region", "WB_income_2022"]
    tb_area = tb_area.drop(columns=columns_to_drop)
    tb_population = tb_population.drop(columns=columns_to_drop)

    # Melt to get 'area' and 'year' in a single column, keeping the 'country' column intact
    tb_area = tb_area.melt(id_vars=["GADM_Name"], var_name="area_year", value_name="value")
    # Extract 'area' and 'year' from the combined 'area_year' column
    tb_area["indicator"] = tb_area["area_year"].str.extract(r"([A-Za-z_]+)")
    tb_area["year"] = tb_area["area_year"].str.extract(r"(\d{4})")
    tb_area = tb_area.drop(columns=["area_year"])

    # Melt to get 'population' and 'year' in a single column, keeping the GADM_Name (country) column intact
    tb_population = tb_population.melt(id_vars=["GADM_Name"], var_name="population_year", value_name="value")
    # Extract 'population' and 'yar' from the combined 'population_year' column
    tb_population["indicator"] = tb_population["population_year"].str.extract(r"([A-Za-z_]+)")
    tb_population["year"] = tb_population["population_year"].str.extract(r"(\d{4})")
    tb_population = tb_population.drop(columns=["population_year"])

    tb = pr.concat([tb_area, tb_population], axis=0, ignore_index=True)
    # Define the renaming dictionary
    renaming_dict = {
        "UC_AREA_": "urban_centre_area",
        "UCL_AREA_": "urban_cluster_area",
        "RUR_AREA_": "rural_total_area",
        "UC_POP_": "urban_centre_population",
        "UCL_POP_": "urban_cluster_population",
        "RUR_POP_": "rural_total_population",
    }

    # Apply the renaming by replacing the entries in the 'indicator' column to be more interpertable
    tb["indicator"] = tb["indicator"].replace(renaming_dict, regex=True)

    tb = tb.rename(columns={"GADM_Name": "country"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
