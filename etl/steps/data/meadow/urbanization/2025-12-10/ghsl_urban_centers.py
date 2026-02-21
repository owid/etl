"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# City size cutoffs (in population).
CITY_SIZE_CUTOFFS = {
    "below_300k": (0, 300000),
    "300k_500k": (300000, 500000),
    "500k_1m": (500000, 1000000),
    "300k_1m": (300000, 1000000),
    "1m_3m": (1000000, 3000000),
    "3m_5m": (3000000, 5000000),
    "1m_5m": (1000000, 5000000),
    "above_5m": (5000000, float("inf")),
    "5m_10m": (5000000, 10000000),
    "above_10m": (10000000, float("inf")),
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_urban_centers.xlsx")

    # Load data from snapshot (only once).
    tb_raw = snap.read(safe_types=False, sheet_name="UC_STATS")

    # Process data.
    #
    # Rename columns to be more interpretable.
    tb_raw = tb_raw.rename(
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

    # Replace zeros with NaNs in the urban_pop column (when the urban center did not meet the criteria).
    tb_raw["urban_pop"] = tb_raw["urban_pop"].replace(0, pd.NA)
    # Convert the urban_pop column to a numeric dtype.
    tb_raw["urban_pop"] = pd.to_numeric(tb_raw["urban_pop"], errors="coerce")

    # Filter the Table where urban_center_name is NaN or "N/A".
    tb_raw = tb_raw.dropna(subset=["urban_center_name"])
    tb_raw = tb_raw[tb_raw["urban_center_name"] != "N/A"]

    # Create working table with selected columns for capitals/top 100.
    tb = tb_raw[["ID_MTUC_G0", "country", "urban_center_name", "capital", "year", "urban_pop", "urban_area"]].copy()

    # Calculate urban density.
    tb["urban_density"] = tb["urban_pop"] / tb["urban_area"]

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

    # Create city size aggregates using already-loaded data.
    tb_all_cities = tb_raw[["country", "year", "urban_pop"]].copy()

    # Drop missing population values.
    tb_all_cities = tb_all_cities.dropna(subset=["urban_pop"])
    tb_all_cities = tb_all_cities[tb_all_cities["urban_pop"] > 0]

    # Create columns for each city size category using vectorized operations.
    urban_pop = tb_all_cities["urban_pop"]
    for size_name, (min_pop, max_pop) in CITY_SIZE_CUTOFFS.items():
        mask = (urban_pop >= min_pop) & (urban_pop < max_pop)
        tb_all_cities[f"pop_{size_name}"] = urban_pop.where(mask, 0)

    # Add aggregate columns calculated directly from raw data.
    # 300k or more.
    tb_all_cities["pop_above_300k"] = urban_pop.where(urban_pop >= 300000, 0)
    # 1 million or more.
    tb_all_cities["pop_above_1m"] = urban_pop.where(urban_pop >= 1000000, 0)

    # Aggregate by country and year.
    agg_dict = {f"pop_{size_name}": "sum" for size_name in CITY_SIZE_CUTOFFS.keys()}
    agg_dict["pop_above_300k"] = "sum"
    agg_dict["pop_above_1m"] = "sum"
    tb_city_sizes = tb_all_cities.groupby(["country", "year"], as_index=False)[list(agg_dict.keys())].sum()

    # Merge capital, top 100, and city size tables.
    tb = pr.merge(tb_capitals, tb_top, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_city_sizes, on=["country", "year"], how="outer")

    # Ensure metadata is propagated.
    metadata_cols = ["urban_pop", "urban_density", "urban_density_top_100", "urban_pop_top_100"]
    # Add city size columns.
    metadata_cols.extend([f"pop_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    # Add aggregate columns.
    metadata_cols.extend(["pop_above_300k", "pop_above_1m"])

    for col in metadata_cols:
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
