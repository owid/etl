"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("monthly_fire_emissions")

    # Read table from garden dataset.
    tb = ds_garden["monthly_fire_emissions"]
    #
    # Process data.
    #
    tb = tb.reset_index()
    tb = tb.drop(columns=["month", "year"])
    #  Use the days since colimn instead of year and month for grapher
    tb = tb.rename(columns={"days_since_2000": "year"})
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
