"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load inputs.
    snap = paths.load_snapshot("ghsl_degree_of_urbanisation.xlsx")

    # Define columns to drop.
    columns_to_drop = ["IDs", "GADM_ISO", "sdg_region", "WB_income_2022"]

    # Load and process data from sheets.
    tb_area = load_and_process_sheet(snap, "AREA_km2_L1", columns_to_drop)
    tb_population = load_and_process_sheet(snap, "POP_L1", columns_to_drop)

    # Concatenate area and population data.
    tb = pr.concat([tb_area, tb_population], axis=0, ignore_index=True)

    # Define the renaming dictionary.
    renaming_dict = {
        "UC_AREA_": "urban_centre_area",
        "UCL_AREA_": "urban_cluster_area",
        "RUR_AREA_": "rural_total_area",
        "UC_POP_": "urban_centre_population",
        "UCL_POP_": "urban_cluster_population",
        "RUR_POP_": "rural_total_population",
    }

    # Apply the renaming by replacing the entries in the 'indicator' column to be more interpretable.
    tb["indicator"] = tb["indicator"].replace(renaming_dict, regex=True)

    # Rename columns.
    tb = tb.rename(columns={"GADM_Name": "country"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator"])

    # Save outputs.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()


def load_and_process_sheet(snap, sheet_name: str, columns_to_drop: list) -> Table:
    # Load data from snapshot.
    tb = snap.read(sheet_name=sheet_name)

    # Remove rows where all values are NaNs.
    tb = tb.dropna(how="all")

    # Drop unnecessary columns.
    tb = tb.drop(columns=columns_to_drop)

    # Melt to get 'value' and 'year' in a single column, keeping the 'country' column intact.
    tb = tb.melt(id_vars=["GADM_Name"], var_name="indicator_year", value_name="value")

    # Extract 'indicator' and 'year' from the combined 'indicator_year' column.
    tb["indicator"] = tb["indicator_year"].str.extract(r"([A-Za-z_]+)")
    tb["year"] = tb["indicator_year"].str.extract(r"(\d{4})")
    tb = tb.drop(columns=["indicator_year"])

    return tb
