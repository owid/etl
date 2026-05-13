"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Columns that don't make sense to plot in grapher.
DROP_COLS = [
    "at_what_time_of_the_year_is_influenza_vaccine_generally_offered",
    "is_influenza_vaccination_recommended_for_other_groups",
    "what_are_the_other_vaccine_types_used",
    "what_time_period_are_influenza_vaccination_policy_and_vaccine_availability_reported_on",
]


def run() -> None:
    ds_garden = paths.load_dataset("flu_vaccine_policy")
    tb = ds_garden.read("flu_vaccine_policy", reset_index=False)
    tb = tb.drop(columns=DROP_COLS)

    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
