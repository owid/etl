"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("gbd_child_mortality")

    # Read table from meadow dataset.
    tb = ds_garden.read("gbd_child_mortality_deaths")
    # Exclude rows where the cause is "All causes"
    tb = tb[tb["cause"] != "All causes"]

    # Group by 'country', 'year', 'sex', and 'age_group' and find the cause with the maximum death rate
    tb = tb.loc[tb.groupby(["country", "year", "sex", "age", "metric"])["value"].idxmax()]
    tb = tb.drop(columns=["value"])

    # Format the tables
    tb = tb.format(["country", "year", "metric", "age", "sex"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
