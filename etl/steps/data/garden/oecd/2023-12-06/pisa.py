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
    # Calculate upper and lower bounds
    tb["reading_lower_bound"] = tb["pisa_reading_all_average"] - 1.96 * tb["pisa_reading_all_se"]
    tb["reading_upper_bound"] = tb["pisa_reading_all_average"] + 1.96 * tb["pisa_reading_all_se"]
    tb["science_lower_bound"] = tb["pisa_science_all_average"] - 1.96 * tb["pisa_science_all_se"]
    tb["science_upper_bound"] = tb["pisa_science_all_average"] + 1.96 * tb["pisa_science_all_se"]
    tb["math_lower_bound"] = tb["pisa_math_all_average"] - 1.96 * tb["pisa_math_all_se"]
    tb["math_upper_bound"] = tb["pisa_math_all_average"] + 1.96 * tb["pisa_math_all_se"]

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
    # Add metadata.
    for column in tb.columns:
        subject = ""
        if "math" in column:
            subject = "math"
        elif "reading" in column:
            subject = "reading"
        elif "science" in column:
            subject = "science"

        sex = ""
        if "all" in column:
            sex = "students"
        elif "_girls" in column:
            sex = "female students"
        elif "_boys" in column:
            sex = "male students"

        tb[column].metadata.display = {}
        tb[column].metadata.display["numDecimalPlaces"] = 0
        tb[column].metadata.description_short = (
            f"Assessed through the PISA {subject} scale, which is based on an OECD country mean of 500 points "
            "and a standard deviation of 100 points."
        )
        if "bound" not in column:
            # Regular column metadata
            tb[column].metadata.unit = "score"
            tb[column].metadata.description_from_producer = (
                f"Average score of 15-year-old {sex} on the PISA {subject} scale. "
                f"The metric for the overall {subject} scale is based on a mean for OECD countries "
                "of 500 points and a standard deviation of 100 points. Data reflects country performance "
                "in the stated year according to PISA reports, but may not be comparable across years or countries. "
                "Consult the PISA website for more detailed information: http://www.oecd.org/pisa/"
            )
            tb[column].metadata.title = f"Average performance of 15-year-old {sex} on the {subject} scale"
            tb[column].metadata.display["name"] = f"Mean {subject} score"
            tb[column].metadata.processing_level = "minor"

        else:
            # Bound column metadata
            bound_type = "upper" if "upper" in column else "lower"
            tb[column].metadata.unit = "score"
            tb[column].metadata.description_from_producer = (
                f"This represents the {bound_type} bound of the average {subject} score for 15-year-old {sex} on the PISA scale. "
                f"The metric for the overall {subject} scale is based on a mean for OECD countries "
                "of 500 points and a standard deviation of 100 points. Data reflects country performance "
                "in the stated year according to PISA reports, but may not be comparable across years or countries. "
                "Consult the PISA website for more detailed information: http://www.oecd.org/pisa/"
            )
            tb[
                column
            ].metadata.title = (
                f"{bound_type.capitalize()} bound of performance of 15-year-old {sex} on the {subject} scale"
            )

            tb[
                column
            ].metadata.description_processing = f"Calculated as the average {subject} score {'plus' if bound_type == 'upper' else 'minus'} 1.96 times the standard error, representing the {bound_type} bounds of a 95% confidence interval."
            tb[column].metadata.processing_level = "major"
            tb[column].metadata.display["name"] = f"{bound_type.capitalize()} bound {subject} score"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
