"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_urban_centers.xlsx")

    # Load data from snapshot.
    tb_urban_center_names = snap.read(safe_types=False, sheet_name="General_info")
    tb_urban_center_density = snap.read(safe_types=False, sheet_name="Area_km2_time_series")
    tb_urban_center_population = snap.read(safe_types=False, sheet_name="POP_time_series")

    # Process data.
    #

    # Remove duplicates in the ID sheet - based on the name of the urban center and country
    tb_urban_center_names = tb_urban_center_names.drop_duplicates(subset=["Main Name", "GADM_name"])

    tb_urban_center_names = tb_urban_center_names[
        [
            "ID_MTUC_G0",
            "Main Name",
            "GADM_name",
            "UNSDGRegion",
            "CountryCapital",
        ]
    ]
    tb_urban_center_density = tb_urban_center_density.melt(
        id_vars=["ID_MTUC_G0"], var_name="year", value_name="urban_area"
    )
    tb_urban_center_population = tb_urban_center_population.melt(
        id_vars=["ID_MTUC_G0"], var_name="year", value_name="urban_pop"
    )

    # Replace zeros with NaNs in the urban_pop column (when the urban center did not meet the criteria)
    tb_urban_center_population["urban_pop"] = tb_urban_center_population["urban_pop"].replace(0, pd.NA)

    # Convert the urban_pop column to a numeric dtype
    tb_urban_center_population["urban_pop"] = pd.to_numeric(tb_urban_center_population["urban_pop"], errors="coerce")

    tb = pr.merge(
        tb_urban_center_population,
        tb_urban_center_density,
        on=["ID_MTUC_G0", "year"],
        how="outer",
    )
    tb["urban_density"] = tb["urban_pop"] / tb["urban_area"]

    tb = pr.merge(
        tb,
        tb_urban_center_names,
        on="ID_MTUC_G0",
        how="right",
    )

    tb = tb.rename(
        columns={
            "GADM_name": "country",
            "Main Name": "urban_center_name",
            "UNSDGRegion": "region",
            "WBIncome2022": "income_group",
            "CountryCapital": "capital",
        }
    )

    # Filter the Table where urban_center_name is NaN
    tb = tb.dropna(subset=["urban_center_name"])

    # Population and density of the capital city
    tb_capitals = tb[tb["capital"] == 1]

    tb_capitals = tb_capitals.drop(columns=["ID_MTUC_G0", "region", "capital"])

    # Select the top 100 most populous cities in 2020
    tb_2020 = tb[tb["year"] == 2020]
    top_100_pop_2020 = tb_2020.nlargest(100, "urban_pop").drop_duplicates(subset=["ID_MTUC_G0"])

    # Filter the original Table to select the top urban centers
    tb_top = tb[tb["ID_MTUC_G0"].isin(top_100_pop_2020["ID_MTUC_G0"])]

    tb_top = tb_top.drop(columns=["urban_area", "ID_MTUC_G0", "region", "capital"])
    tb_top = tb_top.rename(columns={"urban_density": "urban_density_top_100", "urban_pop": "urban_pop_top_100"})

    # Format the country column
    tb_top["country"] = tb_top["urban_center_name"] + " (" + tb_top["country"] + ")"
    tb_top = tb_top.drop(columns=["urban_center_name"])

    tb = pr.merge(tb_capitals, tb_top, on=["country", "year"], how="outer")

    for col in ["urban_pop", "urban_density_top_100", "urban_pop_top_100"]:
        tb[col].metadata.origins = tb["country"].metadata.origins

    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
