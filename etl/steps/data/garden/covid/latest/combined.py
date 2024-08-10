"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_tests = paths.load_dataset("testing")
    ds_who = paths.load_dataset("cases_deaths")

    # Read table from meadow dataset.
    tb_tests = ds_tests["testing"].reset_index()
    tb_who = ds_who["cases_deaths"].reset_index()

    #
    # Process data.
    #
    # Keep relevant columns, set dtypes
    # [["country", "date", "total_tests", "new_tests_7day_smoothed"]]
    tb_tests = tb_tests.astype(
        {
            "date": "datetime64[ns]",
            "total_tests": float,
            "new_tests_7day_smoothed": float,
        }
    )
    # [["country", "date", "total_cases", "new_cases_7_day_avg_right"]]
    tb_cases = tb_who.astype(
        {
            "date": "datetime64[ns]",
            "total_cases": float,
            "new_cases_7_day_avg_right": float,
        }
    )
    # Merge
    tb = tb_tests.merge(tb_cases, on=["country", "date"])

    # Estimate indicators
    ## Tests per cases
    tb["cumulative_tests_per_case"] = tb["total_tests"] / tb["total_cases"]
    tb["short_term_tests_per_case"] = tb["new_tests_7day_smoothed"] / tb["new_cases_7_day_avg_right"]
    ## Cases per tests
    tb["short_term_positivity_rate"] = 100 * tb["new_cases_7_day_avg_right"] / tb["new_tests_7day_smoothed"]
    tb["cumulative_positivity_rate"] = 100 * tb["total_cases"] / tb["total_tests"]

    # Keep relevant columns
    tb = tb[
        [
            "country",
            "date",
            "cumulative_tests_per_case",
            "short_term_tests_per_case",
            "short_term_positivity_rate",
            "cumulative_positivity_rate",
        ]
    ]

    # Format
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
