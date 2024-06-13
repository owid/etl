"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("guardian_mentions")

    # Read table from meadow dataset.
    tb = ds_meadow["guardian_mentions"].reset_index()

    #
    # Process data.
    #
    ## Harmonize countries
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    ## Get relative values
    tb = add_relative_indicator(tb, "num_pages_tags")
    tb = add_relative_indicator(tb, "num_pages_mentions")

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


def add_relative_indicator(tb, colname):
    """Add relative indicator.

    E.g. Global share of ? for a given year. Note that we use 'per 100,000' factor.
    """
    tb["total"] = tb.groupby("year")[colname].transform(sum)
    tb[f"{colname}_relative"] = tb[colname] / tb["total"] * 100_000
    tb = tb.drop(columns=["total"])
    return tb
