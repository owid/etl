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
    # Retrieve snapshots for 2024 and 2023 data.
    snap_2024 = paths.load_snapshot("ai_adoption.csv")
    snap_2023 = paths.load_snapshot("ai_adoption_2023.csv")

    # Load data from snapshot.
    tb_2024 = snap_2024.read()
    tb_2023 = snap_2023.read()

    #
    # Process data.
    #
    tb_2023["% of Respondents"] *= 100
    tb_2023 = tb_2023.rename(columns={"Geographic Area": "country", "% of Respondents": "pct_of_respondents"})
    # Select the rows where 'year' is 2021 as 2022 is already in 2024 AI index data
    tb_2021 = tb_2023[tb_2023["Year"] == 2021].copy()

    tb_2024["% of respondents"] = tb_2024["% of respondents"].str.replace("%", "")
    tb_2024 = tb_2024.rename(columns={"Geographic Area": "country", "% of respondents": "pct_of_respondents"})

    tb_2021.rename(columns={"Geographic Area": "country"}, inplace=True)

    # Ensure the names of the regions are consistent with the 2024 AI index report data
    tb_2021["country"] = tb_2021["country"].replace(
        {
            "Developing Markets (incl. India, Latin America, MENA)": "Developing Markets  (incl. India,  Latin America,  MENA)",
            "Greater China (incl. Hong Kong, Taiwan)": "Greater China  (incl. Hong Kong,  Taiwan)",
            "All Geographies": "All geographies",
        }
    )

    tb = pr.concat([tb_2024, tb_2021])

    # Harmonize the country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_2024.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
