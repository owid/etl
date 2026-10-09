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

    sanity_check_inputs(tb)

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

    sanity_check_outputs(tb, tb_sum)

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


def sanity_check_inputs(tb) -> None:
    assert not tb.duplicated(subset=["country", "year", "description"]).any(), (
        "Duplicate (country, year, description) rows in meadow input."
    )
    # SCHEDULE_MAPPING.replace() silently leaves any unmapped raw code untouched instead of
    # raising -- this is how "Yes (VA)" went unnoticed until manual inspection. Catch the next
    # one loudly instead.
    unexpected_codes = set(tb["intro"].dropna().unique()) - set(SCHEDULE_MAPPING.keys())
    assert not unexpected_codes, (
        f"Unexpected status code(s) in intro column, not covered by SCHEDULE_MAPPING: {unexpected_codes}. "
        "Add them to SCHEDULE_MAPPING (confirming their meaning with WHO first) before proceeding."
    )


def sanity_check_outputs(tb, tb_sum) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output table has a fully-NaN column."
    # No raw code should have leaked through unmapped into the final intro column.
    valid_values = {v for v in SCHEDULE_MAPPING.values() if pd.notna(v)}
    unexpected_values = set(tb["intro"].dropna().unique()) - valid_values
    assert not unexpected_values, f"Unmapped raw code(s) leaked into output intro column: {unexpected_values}"
    assert (tb_sum["countries"] >= 0).all(), "Negative country count in vaccination_introductions_sum."
