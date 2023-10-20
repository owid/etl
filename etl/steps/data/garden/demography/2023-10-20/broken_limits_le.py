"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Year to start tracking. Note that in the first years, few countries have data. Hence, we start in a later year, where more countries have data.
YEAR_FIRST = 1840


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb = ds_meadow["hmd"]

    #
    # Process data.
    #
    # Filter relevant dimensions
    tb = tb.loc[("1x1", "period", slice(None), slice(None), slice(None), "0"), ["life_expectancy"]].reset_index()

    # Keep relevant columns and rows
    tb = tb.drop(columns=["format", "type", "age"]).dropna()

    # Dtypes
    tb["year"] = tb["year"].astype(str).astype(int)

    # Get max for each year
    tb = tb.loc[tb.groupby(["year", "sex"], observed=True)["life_expectancy"].idxmax()]

    # Organise columns
    tb["country_with_max_le"] = tb["country"]
    tb["country"] = tb["country"].astype(str) + " " + tb["year"].astype(str)

    # First year
    tb = tb[tb["year"] >= YEAR_FIRST]

    # Set index
    tb = tb.set_index(["country", "year", "sex"], verify_integrity=True)
    tb.m.short_name = "broken_limits_le"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
