"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("famines_by_regime_gdp")

    # Read table from garden dataset.
    tb = ds_garden["famines"].reset_index()

    #
    # Process data.
    #

    tb = tb.rename({"famine_name": "country"}, axis=1)

    # Keep the first year for each famine (country)
    tb = tb.sort_values(by="year").drop_duplicates(subset="country", keep="first")
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
