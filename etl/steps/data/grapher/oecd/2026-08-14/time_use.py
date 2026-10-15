"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("time_use")

    tb = ds_garden.read("time_use")

    #
    # Process data.
    #
    # The survey-year and reference-age labels are strings for context, not indicators.
    tb = tb.drop(columns=["survey_year", "age_of_reference"])
    tb = tb.format(["country", "year", "sex"], short_name="time_use")

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()
