"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define grouped years to not repeat values
GROUPED_YEARS = {
    2006: 2007,
    2007: 2007,
    2008: 2010,
    2009: 2010,
    2010: 2010,
    2011: 2013,
    2012: 2013,
    2013: 2013,
    2014: 2016,
    2015: 2016,
    2016: 2016,
    2017: 2019,
    2018: 2019,
    2019: 2019,
    2020: 2022,
    2021: 2022,
    2022: 2022,
    2023: 2024,
    2024: 2024,
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("trust_in_government")

    # Read table from meadow dataset.
    tb = ds_meadow.read("trust_in_government")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Group years to not repeat values.
    tb["year"] = tb["year"].replace(GROUPED_YEARS)

    # Drop duplicates created by grouping years.
    tb = tb.drop_duplicates(subset=["country", "year"], keep="last")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
