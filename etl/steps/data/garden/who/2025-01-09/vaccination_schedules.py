"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


SCHEDULE_MAPPING = {
    "Yes": "Entire country",
    "No": "Not routinely administered",
    "Yes (P)": "Regions of the country",
    "Yes (R)": "Specific risk groups",
    "Yes (A)": "Adolescents",
    "Yes (O)": "During outbreaks",
    "Yes (S)": "Administered sequentially",
    "Yes (OPV)": "When IPV and OPV are co-administered",
    "High risk area": "High risk areas",
    "Yes (D)": "Demonstration projects",
    "ND": pd.NA,
    "NR": pd.NA,
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccination_schedules")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccination_schedules")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Use the mapping to replace the values in the intro column.
    tb["intro"] = tb["intro"].replace(SCHEDULE_MAPPING)
    tb = tb.drop(columns=["iso_3_code", "who_region"])

    # Calculate the number of countries administering the vaccine.
    tb_sum = tb[tb["intro"].isin(["Entire country", "Regions of the country", "Specific risk groups", "Adolescents"])]
    tb_sum = tb_sum.groupby(["year", "description"])["intro"].count().reset_index()
    tb_sum["country"] = "World"
    tb_sum = tb_sum.rename(columns={"intro": "countries"})

    tb = tb.format(["country", "year", "description"])
    tb_sum = tb_sum.format(["country", "year", "description"], short_name="vaccination_schedules_sum")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
