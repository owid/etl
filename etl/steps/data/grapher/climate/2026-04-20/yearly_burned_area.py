"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("monthly_burned_area")

    # Read table from garden dataset.
    tb = ds_garden["monthly_burned_area"]
    #
    # Process data.
    #
    tb = tb.reset_index()
    tb = tb.drop(columns=["days_since_2000"])

    # Sum the burned area by country and year
    grouped_tb = (
        tb.groupby(
            [
                "country",
                "year",
            ],
            observed=True,
        )[["forest", "savannas", "shrublands_grasslands", "croplands", "other", "all"]]
        .sum()
        .reset_index()
    )

    grouped_tb = grouped_tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[grouped_tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
