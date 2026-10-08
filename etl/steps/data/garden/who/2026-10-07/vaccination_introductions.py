"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

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
    "Yes (M)": "Maternal vaccination only",
    "Yes (I)": "Infant antibodies only",
    "Yes (Both)": "Both",
    # NOTE: "VA" is not defined in the source file and only appears on Seasonal Influenza rows. I've emailed WHO (vpdata@who.int), but until we hear back, keep the raw code visible rather than guessing a label.
    "Yes (VA)": "Yes (VA) (unknown meaning)",
    "ND": pd.NA,
    "NR": pd.NA,
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccination_introductions")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccination_introductions")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)
    # Use the mapping to replace the values in the intro column.
    tb["intro"] = tb["intro"].replace(SCHEDULE_MAPPING)
    tb = tb.drop(columns=["iso_3_code", "who_region"])

    # Calculate the number of countries administering the vaccine.
    tb_sum = tb[
        tb["intro"].isin(
            [
                "Entire country",
                "Regions of the country",
                "Specific risk groups",
                "Adolescents",
                "Maternal vaccination only",
                "Infant antibodies only",
                "Both",
            ]
        )
    ]
    tb_sum = tb_sum.groupby(["year", "description"])["intro"].count().reset_index()
    tb_sum["country"] = "World"
    tb_sum = tb_sum.rename(columns={"intro": "countries"})

    tb = tb.format(["country", "year", "description"])
    tb_sum = tb_sum.format(["country", "year", "description"], short_name="vaccination_introductions_sum")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_sum], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
