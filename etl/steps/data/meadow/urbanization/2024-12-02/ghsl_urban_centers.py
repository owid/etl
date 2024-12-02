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
    snap = paths.load_snapshot("ghsl_urban_centers.xlsx")

    # Load data from snapshot.
    tb_urban_center_names = snap.read(safe_types=False, sheet_name="General_info")
    tb_urban_center_density = snap.read(safe_types=False, sheet_name="Area_km2_time_series")
    tb_urban_center_population = snap.read(safe_types=False, sheet_name="POP_time_series")

    # Process data.
    #
    # Remove duplicates in the ID sheet
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
    tb = tb[tb["capital"] == 1]
    tb = tb.drop(columns=["ID_MTUC_G0", "region", "capital"])

    tb = tb.format(["country", "year", "urban_center_name"])

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
