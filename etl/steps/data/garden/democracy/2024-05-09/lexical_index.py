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
    tb = tb[
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
            "poliberties_liead",
        ]
    ]

    # Format
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
