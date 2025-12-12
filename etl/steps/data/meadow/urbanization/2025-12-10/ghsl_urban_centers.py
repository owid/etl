"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_urban_centers.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False, sheet_name="UC_STATS")

    # Process data.
    #
    # Rename columns to be more interpretable.
    tb = tb.rename(
        columns={
            "UNLocName": "country",
            "UCname": "urban_center_name",
            "Year": "year",
            "AREA_km2": "urban_area",
            "POP": "urban_pop",
            "BU_km2": "built_up_area",
            "CapitalFlag": "capital",
            "ID_UC_G0": "ID_MTUC_G0",
        }
    )

    # Select relevant columns.
    tb = tb[["ID_MTUC_G0", "country", "urban_center_name", "capital", "year", "urban_pop", "urban_area"]]

    # Replace zeros with NaNs in the urban_pop column (when the urban center did not meet the criteria).
    tb["urban_pop"] = tb["urban_pop"].replace(0, pd.NA)
    # Convert the urban_pop column to a numeric dtype.
    tb["urban_pop"] = pd.to_numeric(tb["urban_pop"], errors="coerce")

    # Calculate urban density.
    tb["urban_density"] = tb["urban_pop"] / tb["urban_area"]

    # Filter the Table where urban_center_name is NaN or "N/A".
    tb = tb.dropna(subset=["urban_center_name"])
    tb = tb[tb["urban_center_name"] != "N/A"]

    # Population and density of the capital city.
    # Some countries have multiple capitals - select the official/administrative capital.
    tb_capitals = tb[tb["capital"] == 1].copy()

    # Define which capital to use for countries with multiple capitals.
    capital_preference = {
        "Benin": "Porto-Novo",
        "Bolivia (Plurinational State of)": "La Paz",
        "Burundi": "Gitega",
        "Chile": "Santiago",
        "Côte d'Ivoire": "Yamoussoukro",
        "Malaysia": "Putrajaya",
        "Netherlands": "The Hague",
        "United Republic of Tanzania": "Dodoma",
        "Yemen": "Şan'ā' (Sana'a)",
        "India": "New Delhi",
        "Pakistan": "Islāmābād",
        "South Africa": "Pretoria",
    }

    # Filter to keep only preferred capitals for multi-capital countries (vectorized).
    multi_capital_mask = tb_capitals["country"].isin(capital_preference.keys())

    # For multi-capital countries, create a mask for preferred capitals.
    tb_capitals["preferred_capital"] = tb_capitals["country"].map(capital_preference)
    preferred_mask = tb_capitals["urban_center_name"] == tb_capitals["preferred_capital"]

    # Keep all single-capital countries OR preferred capitals from multi-capital countries.
    tb_capitals = tb_capitals[~multi_capital_mask | preferred_mask].copy()
    tb_capitals = tb_capitals.drop(columns=["ID_MTUC_G0", "capital", "preferred_capital"])
    # Select the top 100 most populous cities in 2020.
    tb_2020 = tb[tb["year"] == 2020]
    top_100_pop_2020 = tb_2020.nlargest(100, "urban_pop").drop_duplicates(subset=["ID_MTUC_G0"])

    # Filter the original Table to select the top urban centers.
    tb_top = tb[tb["ID_MTUC_G0"].isin(top_100_pop_2020["ID_MTUC_G0"])].copy()
    tb_top = tb_top.drop(columns=["urban_area", "ID_MTUC_G0", "capital"])
    tb_top = tb_top.rename(columns={"urban_density": "urban_density_top_100", "urban_pop": "urban_pop_top_100"})

    # Format the country column for top 100.
    tb_top["country"] = tb_top["urban_center_name"] + " (" + tb_top["country"] + ")"
    tb_top = tb_top.drop(columns=["urban_center_name"])

    # Merge capital and top 100 tables.
    tb = pr.merge(tb_capitals, tb_top, on=["country", "year"], how="outer")

    # Ensure metadata is propagated.
    for col in ["urban_pop", "urban_density", "urban_density_top_100", "urban_pop_top_100"]:
        if col in tb.columns:
            tb[col].metadata.origins = tb["country"].metadata.origins

    # Format the table.
    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
