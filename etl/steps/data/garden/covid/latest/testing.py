"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("testing")

    # Read table from meadow dataset.
    tb = ds_meadow["testing"].reset_index()

    #
    # Process data.
    #
    # Rename
    tb = tb.rename(
        columns={
            "date": "date",
            "cumulative_total": "total_tests",
            "cumulative_total_per_thousand": "total_tests_per_thousand",
            "daily_change_in_cumulative_total": "new_tests",
            "daily_change_in_cumulative_total_per_thousand": "new_tests_per_thousand",
            "_7_day_smoothed_daily_change": "new_tests_7day_smoothed",
            "_7_day_smoothed_daily_change_per_thousand": "new_tests_per_thousand_7day_smoothed",
        }
    )
    # Drop columns
    ## These columns are out of sync, we will generate them in another Garden dataset with combined metrics
    tb = tb.drop(
        columns=[
            "short_term_positive_rate",
            "short_term_tests_per_case",
            "entity",
            "iso_code",
            "source_url",
            "source_label",
            "notes",
            "tests_units",
        ]
    )

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
