"""Load a garden dataset and create a grapher dataset."""

from etl.grapher import helpers as gh
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def jinja_unit(metric):
    if metric == "Number":
        return "cases"
    elif metric == "Rate":
        return "cases per 100,000 people"
    elif metric == "Share":
        return "%"


def jinja_short_unit(metric: str) -> str:
    if metric == "Share":
        return "%"
    # For "Rate" and "Number," there's no short unit
    return ""


def jinja_sex(sex: str) -> str:
    if sex == "Both":
        return "individuals"
    elif sex == "Male":
        return "males"
    elif sex == "Female":
        return "females"
    return "unknown"


def jinja_title(metric: str, cause: str, sex: str, age: str) -> str:
    cause_lower = cause.lower()

    # Number
    if metric == "Number":
        if age not in ["Age-standardized", "All ages"]:
            return f"Current cases of {cause_lower}, among {sex} aged {age}"
        elif age == "Age-standardized":
            return f"Age-standardized current number of {sex} with {cause_lower}"
        elif age == "All ages":
            return f"Total current number of {sex} with {cause_lower}"

    # Rate
    elif metric == "Rate":
        if age not in ["Age-standardized", "All ages"]:
            return f"Current cases of {cause_lower}, among {sex} aged {age}, per 100,000 people"
        elif age == "Age-standardized":
            return f"Age-standardized current cases of {sex} with {cause_lower}, per 100,000 people"
        elif age == "All ages":
            return f"Total current number of {sex} with {cause_lower}, per 100,000 people"

    # Share
    elif metric == "Share":
        if age not in ["Age-standardized", "All ages"]:
            return f"Current cases of {cause_lower}, among {sex} aged {age}, per 100 people"
        elif age == "Age-standardized":
            return f"Age-standardized current cases of {sex} with {cause_lower}, per 100 people"
        elif age == "All ages":
            return f"Total current number of {sex} with {cause_lower}, per 100 people"

    # Fallback
    return f"[UNKNOWN TITLE] metric={metric}, age={age}"


def jinja_description_short(metric: str, cause: str, sex: str, age: str) -> str:
    cause_lower = cause.lower()

    # Number
    if metric == "Number":
        if age not in ["Age-standardized", "All ages"]:
            return f"The estimated prevalence of {cause_lower} in {sex} aged {age}."
        elif age == "Age-standardized":
            return f"The estimated age-standardized prevalence of {sex} with {cause_lower}."
        elif age == "All ages":
            return f"The estimated prevalence of {cause_lower} in {sex}."

    # Rate
    elif metric == "Rate":
        if age not in ["Age-standardized", "All ages"]:
            return f"The estimated prevalence of {cause_lower} in {sex} aged {age}, per 100,000 people."
        elif age == "Age-standardized":
            return f"The estimated age-standardized prevalence of {sex} with {cause_lower}, per 100,000 people."
        elif age == "All ages":
            return f"The estimated prevalence of {cause_lower} in {sex}, per 100,000 people."

    # Share
    elif metric == "Share":
        if age not in ["Age-standardized", "All ages"]:
            return f"The estimated prevalence of {cause_lower} in {sex} aged {age}, per 100 people."
        elif age == "Age-standardized":
            return f"The estimated age-standardized prevalence of {sex} with {cause_lower}, per 100 people."
        elif age == "All ages":
            return f"The estimated prevalence of {cause_lower} in {sex}, per 100 people."

    # Fallback
    return f"[UNKNOWN DESCRIPTION] metric={metric}, age={age}"


def jinja_footnote(age: str) -> str:
    if age == "Age-standardized":
        return (
            "To allow for comparisons between countries and over time, "
            "this metric is [age-standardized](#dod:age_standardized)."
        )
    return ""


gh.register_jinja_functions(
    [
        jinja_unit,
        jinja_short_unit,
        jinja_description_short,
        jinja_title,
        jinja_sex,
        jinja_footnote,
    ]
)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gbd_mental_health")

    # Read table from garden dataset.
    tb = ds_garden["gbd_mental_health"]
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
