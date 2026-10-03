"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("relative_poverty_france")

    # Read table from meadow dataset.
    tb = ds_meadow["relative_poverty_france"].reset_index()

    #
    # Process data.
    tb = create_spells(tb)

    tb = tb.format(["country", "year", "spell"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_spells(tb: Table) -> Table:
    """Define spells using a new column"""

    # Split year column into two columns: year and spell. The year column is the first four characters of the year column.
    tb["year_new"] = tb["year"].str[:4]

    # Define spell as boolean where year is the same as the one before.
    tb["spell"] = tb["year_new"] == tb["year_new"].shift(1)

    # Whenever spell is True, set spell to an increasing number.
    tb["spell"] = tb["spell"].cumsum() + 1

    # Drop year and indicator columns and rename year_new to year.
    tb = tb.drop(columns=["year"]).rename(columns={"year_new": "year"})

    return tb
