"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_robots.csv")
    # Load garden dataset from 2023
    ds_garden = paths.load_dataset("ai_robots")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    tb = tb.rename(columns={"Geographic area": "country", "Year": "year"})

    # Harmonize the country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb["Number of robots (in thousands)"] = tb["Number of robots (in thousands)"] * 1000
    tb = tb.rename(columns={"Number of robots (in thousands)": "number_of_robots"})
    tb = tb.pivot(index=["year", "country"], columns="Indicator", values="number_of_robots").reset_index()

    # Load the 2023 data from the meadow dataset
    tb_2024 = ds_garden.read("ai_robots")

    tb_2024 = tb_2024[
        [
            "country",
            "year",
            "agriculture",
            "hospitality",
            "medical_robotics",
            "professional_cleaning",
            "transportation_and_logistics",
        ]
    ]

    tb_2024 = tb_2024.dropna(
        subset=[
            "agriculture",
            "hospitality",
            "medical_robotics",
            "professional_cleaning",
            "transportation_and_logistics",
        ],
        how="all",
    )
    column_rename_map = {
        "agriculture": "Agriculture",
        "hospitality": "Hospitality",
        "medical_robotics": "Medical and health care",
        "professional_cleaning": "Professional cleaning",
        "transportation_and_logistics": "Transportation and logistics",
    }
    # Standardize column names
    tb_2024 = tb_2024.rename(columns=column_rename_map)

    tb_professional = pr.concat([tb[tb_2024.columns], tb_2024])
    tb_professional = tb_professional.drop_duplicates(subset=["country", "year"], keep="last")

    industrial_robots = [
        col
        for col in tb.columns
        if col
        not in [
            "Agriculture",
            "Hospitality",
            "Medical and health care",
            "Professional cleaning",
            "Transportation and logistics",
        ]
    ]

    # Industrial robots
    tb_industrial = tb[industrial_robots].copy()
    tb_industrial = tb_industrial.format(["country", "year"])
    tb_industrial.metadata.short_name = "industrial_robots"

    # Professional service robots
    tb_professional = tb_professional.drop(columns=["country"])
    # Remove rows where all columns except 'year' are NaN
    tb_professional = tb_professional.dropna(
        subset=[col for col in tb_professional.columns if col != "year"], how="all"
    )

    tb_professional = tb_professional.melt(
        id_vars=["year"],
        value_vars=[
            "Agriculture",
            "Hospitality",
            "Medical and health care",
            "Professional cleaning",
            "Transportation and logistics",
        ],
        var_name="application_area",
        value_name="number_of_professional_robots_installed",
    )

    tb_professional = tb_professional.format(["year", "application_area"])
    tb_professional.metadata.short_name = "professional_robots"
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_professional, tb_industrial], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
