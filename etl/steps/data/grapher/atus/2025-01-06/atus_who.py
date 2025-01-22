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

    # Use year for age to work in grapher
    tb = tb.rename(columns={"age": "year"})

    # Drop timeframe column
    tb = tb.drop(columns=["timeframe"])

    # format
    tb = tb.format(["country", "year", "gender", "who_category"])
    tb_years = tb_years.format(["country", "year", "age_bracket", "who_category"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb, tb_years], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
