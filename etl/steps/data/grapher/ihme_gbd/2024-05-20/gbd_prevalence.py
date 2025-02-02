"""Load a garden dataset and create a grapher dataset."""

from etl.grapher import helpers as gh
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def jinja_description_short(metric: str, age: str, cause: str) -> str:
    # Number
    if metric == "Number":
        if age not in ("Age-standardized", "All ages"):
            return f"The estimated number of new cases of {cause} in those aged {age}."
        elif age == "Age-standardized":
            return f"The estimated number of age-standardized new cases of {cause}."
        elif age == "All ages":
            return f"The estimated number of new cases of {cause}."

    # Rate
    elif metric == "Rate":
        if age not in ("Age-standardized", "All ages"):
            return f"The estimated number of new cases of {cause} in those aged {age}, per 100,000 people."
        elif age == "Age-standardized":
            return f"The estimated number of age-standardized new cases of {cause}, per 100,000 people."
        elif age == "All ages":
            return f"The estimated number of new cases of {cause}, per 100,000 people."

    # Share
    elif metric == "Share":
        if age not in ("Age-standardized", "All ages"):
            return f"The estimated number of new cases of {cause} in those aged {age}, per 100 people."
        elif age == "Age-standardized":
            return f"The estimated number of age-standardized new cases of {cause}, per 100 people."
        elif age == "All ages":
            return f"The estimated number of new cases of {cause}, per 100 people."

    # If none of the above conditions matched (fallback)
    return ""


gh.register_jinja_functions(
    [
        jinja_description_short,
    ]
)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gbd_prevalence")
    ds_garden.metadata.title = "Global Burden of Disease - Prevalence"
    # Read table from garden dataset.
    tb_prevalence = ds_garden["gbd_prevalence"]
    # tb_incidence = ds_garden["gbd_incidence"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_prevalence],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
