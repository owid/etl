"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_robots.csv")
    # Load garden dataset from 2023
    ds_meadow = paths.load_dataset("ai_robots")

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
    tb_2023 = ds_meadow.read("ai_robots")
    tb_2023 = tb_2023[
        [
            "country",
            "year",
            "professional_service_robots__number_of_professional_service_robots_installed__in_thousands",
            "professional_service_robots__application_area",
        ]
    ]

    tb_2023 = tb_2023.dropna(
        subset=["professional_service_robots__application_area", "professional_service_robots__application_area"],
        how="all",
    )

    # Convert from thousands to actual number
    tb_2023["number_of_robots_installed"] = (
        tb_2023["professional_service_robots__number_of_professional_service_robots_installed__in_thousands"] * 1000
    )

    tb_2023 = tb_2023.drop(
        columns=["professional_service_robots__number_of_professional_service_robots_installed__in_thousands"]
    )
    tb_2023 = tb_2023.pivot(
        index=["country", "year"],
        columns="professional_service_robots__application_area",
        values="number_of_robots_installed",
    ).reset_index()
    column_rename_map = {
        "Medical Robotics": "Medical and health care",
        "Professional Cleaning": "Professional cleaning",
        "Transportation and Logistics": "Transportation and logistics",
    }
    # Standardize column names
    tb_2023 = tb_2023.rename(columns=column_rename_map)
    tb_professional = pr.concat([tb[tb_2023.columns], tb_2023])
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
    ds_garden = create_dataset(
        dest_dir, tables=[tb_professional, tb_industrial], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
