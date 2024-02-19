"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    print(grouped_tb)

    #  Use the days since colimn instead of year and month for grapher
    grouped_tb = grouped_tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[grouped_tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.metadata.title = "Global Yearly Burned Area"

    # Save changes in the new grapher dataset.
    ds_grapher.save()
