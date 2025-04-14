"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs
    #
    # Retrieve snapshots for 2024 and 2023 data.
    snap_2025 = paths.load_snapshot("ai_adoption.csv")
    snap_2023 = paths.load_snapshot("ai_adoption_2023.csv")

    # Load data from snapshot.
    tb_2025 = snap_2025.read()
    tb_2023 = snap_2023.read()

    #
    # Process data
    #
    tb_2023["% of Respondents"] *= 100
    tb_2023 = tb_2023.rename(columns={"Geographic Area": "country"})

    tb_2025["% of Respondents"] = tb_2025["% of Respondents"].str.replace("%", "").astype(float)
    tb_2025 = tb_2025.rename(columns={"Geographic Area": "country"})

    # Ensure the names of the regions are consistent with the 2024 AI index report data
    tb_2023["country"] = tb_2023["country"].replace(
        {
            "Developing Markets (incl. India, Latin America, MENA)": "India, Latin America, and Middle East and North Africa",
            "Greater China (incl. Hong Kong, Taiwan)": "Greater China",
            "All Geographies": "All geographies",
        }
    )
    tb_2025["country"] = tb_2025["country"].replace(
        {
            "Developing markets (incl. India, Central/South America, MENA)": " India, Latin America, and Middle East and North Africa",
            "Greater China (incl. Hong Kong, Taiwan, Macau)": "Greater China",
            "All Geographies": "All geographies",
        }
    )
    print(tb_2023["country"].unique())
    print(tb_2025["country"].unique())

    tb = pr.concat([tb_2025, tb_2023])

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_2025.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
