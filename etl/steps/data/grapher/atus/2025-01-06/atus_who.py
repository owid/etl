"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("atus_who")

    # Read table from garden dataset.
    tb = ds_garden.read("atus_who")
    tb_years = ds_garden.read("atus_who_years")

    # change wording of gender categories
    tb["gender"] = tb["gender"].replace({"all": "All people", "female": "Women", "male": "Men"})
    tb_years["gender"] = tb_years["gender"].replace({"all": "All people", "female": "Women", "male": "Men"})

    # Use year for age and gender for country to work in grapher
    tb = tb.drop(columns=["country"])
    tb = tb.rename(columns={"age": "year", "gender": "country"})

    tb_years["age_bracket"] = tb_years["age_bracket"].replace(
        {
            "all": "All ages",
            "15-29": "15-29 years",
            "30-44": "30-44 years",
            "45-59": "45-59 years",
            "60-85": "60+ years",
        }
    )

    tb_years = tb_years.drop(columns=["country"])
    tb_years = tb_years.rename(columns={"gender": "country"})

    # Drop timeframe column
    tb = tb.drop(columns=["timeframe"])

    # format
    tb = tb.format(["country", "year", "who_category"])
    tb_years = tb_years.format(["country", "year", "who_category", "age_bracket"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb, tb_years], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
