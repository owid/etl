"""Load a snapshot and create a meadow dataset."""


import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


def rename_columns(tb: Table) -> Table:
    """Rename trust questions columns and separate country and year."""

    # Define dictionary of columns to rename ppltrst trstep trstlgl trstplc trstplt trstprl trstprt trstun gvimpc19 trstsci
    rename_dict = {
        "ppltrst": "trust",
        "trstep": "trust_european_parliament",
        "trstlgl": "trust_legal_system",
        "trstplc": "trust_police",
        "trstplt": "trust_politicians",
        "trstprl": "trust_local_parliament",
        "trstprt": "trust_political_parties",
        "trstun": "trust_united_nations",
        "gvimpc19": "trust_gov_covid19",
        "trstsci": "trust_scientists",
    }

    # Rename columns.
    tb = tb.rename(columns=rename_dict)

    # Extract first two characters from survey column and name it country. Extract last four characters from survey column and name it year.
    tb["country"] = tb["survey"].str[:2]
    tb["year"] = tb["survey"].str[-4:].astype(int)

    # Remove survey column and move country and year to the front.
    tb = tb.drop(columns=["survey"])
    tb = tb[["country", "year"] + list(tb.columns[:-2])]

    # Harmonize ISO2 codes to OWID standard
    tb = harmonize_countries(tb)

    # Set index and verify that it is unique. And sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb


def harmonize_countries(tb: Table) -> Table:
    # Load reference file with country names in OWID standard
    ds_countries_regions = paths.load_dataset("regions")
    tb_countries_regions = ds_countries_regions["regions"].reset_index()

    # Merge dataset and country dictionary to get the name of the country
    tb = pr.merge(
        tb, tb_countries_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left"
    )

    missing_list = list(tb[tb["name"].isnull()]["country"].unique())
    missing_count = len(missing_list)

    # Warns if there are still entities missing
    if missing_count > 0:
        log.warning(
            f"There are still {missing_count} countries/regions without a name and will be deleted! Take a look at this list:\n {missing_list}"
        )

    # Drop rows without match
    tb = tb[~tb["name"].isnull()].reset_index(drop=True)
    # Drop old country and ISO alpha 2 variable. Rename the newly built variable as `country`
    tb = tb.drop(columns=["country", "iso_alpha2"])
    tb = tb.rename(columns={"name": "country"})

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ess_trust.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    tb = rename_columns(tb)

    # Create a new table and ensure all columns are snake-case.
    tb = tb.underscore()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
