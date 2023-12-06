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
    ds_meadow = paths.load_dataset("pisa")
    # Read table from meadow dataset.
    tb = ds_meadow["pisa_math_boys_girls"].reset_index()

    #
    # Process data.
    #

    tb = geo.harmonize_countries(
        df=tb, excluded_countries_file=paths.excluded_countries_path, countries_file=paths.country_mapping_path
    )

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    # Renaming columns to remove '_boys_girls'
    tb = tb.rename(columns=lambda x: x.replace("_boys_girls", ""))
    # Create lower and upper bounds for each variable
    tb["pisa_reading_lower_bound_girls"] = tb["pisa_reading_average_girls"] - tb["pisa_reading_se_girls"]
    tb["pisa_reading_upper_bound_girls"] = tb["pisa_reading_average_girls"] + tb["pisa_reading_se_girls"]

    tb["pisa_reading_lower_bound_boys"] = tb["pisa_reading_average_boys"] - tb["pisa_reading_se_boys"]
    tb["pisa_reading_upper_bound_boys"] = tb["pisa_reading_average_boys"] + tb["pisa_reading_se_boys"]

    tb["pisa_science_lower_bound_girls"] = tb["pisa_science_average_girls"] - tb["pisa_science_se_girls"]
    tb["pisa_science_upper_bound_girls"] = tb["pisa_science_average_girls"] + tb["pisa_science_se_girls"]

    tb["pisa_science_lower_bound_boys"] = tb["pisa_science_average_boys"] - tb["pisa_science_se_boys"]
    tb["pisa_science_upper_bound_boys"] = tb["pisa_science_average_boys"] + tb["pisa_science_se_boys"]

    tb["pisa_math_lower_bound_girls"] = tb["pisa_math_average_girls"] - tb["pisa_math_se_girls"]
    tb["pisa_math_upper_bound_girls"] = tb["pisa_math_average_girls"] + tb["pisa_math_se_girls"]

    tb["pisa_math_lower_bound_boys"] = tb["pisa_math_average_boys"] - tb["pisa_math_se_boys"]
    tb["pisa_math_upper_bound_boys"] = tb["pisa_math_average_boys"] + tb["pisa_math_se_boys"]

    tb["pisa_math_all_lower_bound"] = tb["pisa_math_all_average"] - tb["pisa_math_all_se"]
    tb["pisa_math_all_upper_bound"] = tb["pisa_math_all_average"] + tb["pisa_math_all_se"]

    tb["pisa_science_all_lower_bound"] = tb["pisa_science_all_average"] - tb["pisa_science_all_se"]
    tb["pisa_science_all_upper_bound"] = tb["pisa_science_all_average"] + tb["pisa_science_all_se"]

    tb["pisa_reading_all_lower_bound"] = tb["pisa_reading_all_average"] - tb["pisa_reading_all_se"]
    tb["pisa_reading_all_upper_bound"] = tb["pisa_reading_all_average"] + tb["pisa_reading_all_se"]

    # Remove columns with standard errors
    tb = tb.drop(
        columns=[
            "pisa_reading_se_girls",
            "pisa_reading_se_boys",
            "pisa_science_se_girls",
            "pisa_science_se_boys",
            "pisa_math_se_girls",
            "pisa_math_se_boys",
            "pisa_math_all_se",
            "pisa_science_all_se",
            "pisa_reading_all_se",
        ]
    )
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
