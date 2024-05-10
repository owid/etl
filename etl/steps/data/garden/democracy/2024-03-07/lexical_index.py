"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("lexical_index")

    # Read table from meadow dataset.
    tb = ds_meadow["lexical_index"].reset_index()

    #
    # Process data.
    #
    # Initial cleaning
    tb = preprocess(tb)

    # Format
    tb = tb.format(["country", "year"])

    ######################### WIP
    # Create variable distinguishing between democracies and autocracies:
    mask = tb["regime_redux_lied"] == 6
    tb.loc[mask, "democracy_lied"] = 1
    tb.loc[~mask, "democracy_lied"] = 0

    # Create variable for age of electoral democracies

    # Create variable for age of polyarchies

    # Create variable for experience with electoral democracy

    # Create variable for experience with polyarchy

    # Create variable for age group of electoral demcoracies

    # Create variable for age group of polyarchies

    # Create variable for universal suffrage
    tb.loc[(tb["male_suffrage_lied"] == 0) & (tb["female_suffrage_lied"] == 0), "suffrage_lied"] = 0
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 0), "suffrage_lied"] = 1
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 1), "suffrage_lied"] = 1.5
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 1), "suffrage_lied"] = 2

    assert (
        (tb["suffrage_lied"] == 1.5).sum() == 0
    ), "There are countries with women suffrage but not men suffrage! This is not expected and can lead to confusing visualisations."

    # Add labels for ages of electoral democracies and polyarchies to optimize use in the OWID grapher
    #############################
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def preprocess(tb: Table) -> Table:
    """Pre-process data.

    Includes: removing NaNs, fixing bugs, sanity checks, renaming and selecting relevant columns.
    """
    ## Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    # Rename columns of interest
    tb = rename_columns(tb)

    # HOTFIX 2 -> 1 encoding
    countries_miss_encoded = set(tb.loc[(tb["opposition_lied"] == 2) | (tb["legelec_lied"] == 2), "country"])
    assert countries_miss_encoded == {"Botswana"}
    tb.loc[tb["opposition_lied"] == 2, "opposition_lied"] = 1
    tb.loc[tb["legelec_lied"] == 2, "legelec_lied"] = 1

    # HOTFIX: if regime_lied is 7, then regime_redux_lied should be 6
    # There is an error in Argentina@2022
    tb.loc[(tb["regime_lied"] == 7), "regime_redux_lied"] = 6

    # Select relevant columns
    tb = tb.loc[
        :,
        [
            "country",
            "year",
            "regime_lied",
            "regime_redux_lied",
            "exelec_lied",
            "legelec_lied",
            "opposition_lied",
            "competition_lied",
            "male_suffrage_lied",
            "female_suffrage_lied",
            "poliberties_lied",
        ],
    ]

    return tb


def rename_columns(tb: Table) -> Table:
    """Rename variables of interest."""
    tb = tb.rename(
        columns={
            "executive_elections": "exelec_lied",
            "legislative_elections": "legelec_lied",
            "multi_party_legislative_elections": "opposition_lied",
            "competitive_elections": "competition_lied",
            "political_liberties": "poliberties_lied",
            "lexical_index": "regime_redux_lied",
            "lexical_index_plus": "regime_lied",
            "male_suffrage": "male_suffrage_lied",
            "female_suffrage": "female_suffrage_lied",
        }
    )
    return tb
