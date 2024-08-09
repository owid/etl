"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tackling_inequalities_brazil_2010")

    # Read table from meadow dataset.
    tb = ds_meadow["tackling_inequalities_brazil_2010"].reset_index()

    #
    # Process data.
    #
    # Filter only the dates including "Jun"
    tb = tb[tb["date"].str.contains("Jun")]

    # Create the column year, by extracting the last two characters from the date column.
    tb["year"] = tb["date"].str[-2:].astype(int)

    # Make year 4 digits by adding 2000 if year is less than 50. If not, add 1900.
    tb["year"] = tb["year"].apply(lambda x: x + 2000 if x < 50 else x + 1900)

    # Remove the date column
    tb = tb.drop(columns=["date"])

    # Add country column
    tb["country"] = "Brazil"

    # Multiply gini by 100
    tb["gini"] = tb["gini"] * 100

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
