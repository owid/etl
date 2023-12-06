"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from shared import add_indicators_extra

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Index columns
COLUMNS_INDEX = ["year", "region", "conflict_type"]
COLUMNS_INDEX_COUNTRY = ["year", "country", "conflict_type"]
# Rename columns (has an entry for each dataset. All entries should be dictionaries with the same number of entries (and identical values))
COLUMNS_RENAME = {
    "ucdp": {
        "number_deaths_ongoing_conflicts": "number_deaths_ongoing_conflicts",
        "number_deaths_ongoing_conflicts_high": "number_deaths_ongoing_conflicts_high",
        "number_deaths_ongoing_conflicts_low": "number_deaths_ongoing_conflicts_low",
    },
    "prio": {
        "number_deaths_ongoing_conflicts_battle_low": "number_deaths_ongoing_conflicts_low",
        "number_deaths_ongoing_conflicts_battle_high": "number_deaths_ongoing_conflicts_high",
        "number_deaths_ongoing_conflicts_battle": "number_deaths_ongoing_conflicts",
    },
}
# Indicator columns
COLUMNS_INDICATORS = list(COLUMNS_RENAME["ucdp"].values())
# First year in UCDP
YEAR_UCDP_MIN = 1989


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_ucdp = paths.load_dataset("ucdp")
    # Read table from meadow dataset.
    tb_ucdp = ds_ucdp["ucdp"].reset_index()
    # tb_ucdp_countries = ds_ucdp["ucdp_country"].reset_index()

    # Load meadow dataset.
    ds_prio = paths.load_dataset("prio_v31")
    # Read table from meadow dataset.
    tb_prio = ds_prio["prio_v31"].reset_index()
    # tb_prio_countries = ds_prio["prio_v31_country"].reset_index()

    # Read table from COW codes
    ds_gw = paths.load_dataset("gleditsch")
    tb_regions = ds_gw["gleditsch_regions"].reset_index()

    #
    # Process data.
    #
    ## Remove suffix (PRIO) or (UCDP/PRIO)
    tb_ucdp["region"] = tb_ucdp["region"].str.replace(r" \(.+\)", "", regex=True)
    tb_prio["region"] = tb_prio["region"].str.replace(r" \(.+\)", "", regex=True)

    ## In PRIO, change conflict_type 'all' to 'state-based'
    tb_prio["conflict_type"] = tb_prio["conflict_type"].replace({"all": "state-based"})

    # Sanity checks
    assert set(tb_ucdp["region"]) == set(tb_prio["region"]), "Missmatch in regions between UCDP and PRIO"
    expected_missmatch = {"non-state conflict", "one-sided violence", "all"}
    assert (
        set(tb_ucdp["conflict_type"]) - set(tb_prio["conflict_type"]) == expected_missmatch
    ), "Missmatch in conflict_type between UCDP and PRIO not as expected!"

    # Rename columns, keep relevant indicators
    tb_ucdp = tb_ucdp.rename(columns=COLUMNS_RENAME["ucdp"])[COLUMNS_INDEX + COLUMNS_INDICATORS]
    tb_prio = tb_prio.rename(columns=COLUMNS_RENAME["prio"])[COLUMNS_INDEX + COLUMNS_INDICATORS]

    # Keep relevant years for each dataset
    tb_ucdp = tb_ucdp.dropna(subset=COLUMNS_INDICATORS, how="all")
    assert tb_ucdp["year"].min() == YEAR_UCDP_MIN, "UCDP year min is not as expected!"
    tb_prio = tb_prio[tb_prio["year"] < YEAR_UCDP_MIN]

    # Concatenate
    tb = pr.concat([tb_ucdp, tb_prio], axis=0, ignore_index=True, short_name=paths.short_name)

    # Add conflict rates
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_mortality=[
            "number_deaths_ongoing_conflicts",
            "number_deaths_ongoing_conflicts_high",
            "number_deaths_ongoing_conflicts_low",
        ],
    )

    # tb_country = make_tb_country(tb_ucdp_countries, tb_prio_countries)

    # Set index
    tb = tb.set_index(COLUMNS_INDEX, verify_integrity=True)
    # tb_country = tb_country.set_index(COLUMNS_INDEX_COUNTRY, verify_integrity=True)

    #
    # Save outputs.
    #
    tables = [
        tb,
        # tb_country,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_ucdp.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_tb_country(tb_ucdp_countries: Table, tb_prio_countries: Table) -> Table:
    """Combine UCDP and PRIO country data."""

    # PRIO 'all' conflict is actually 'state-based'
    tb_prio_countries["conflict_type"] = tb_prio_countries["conflict_type"].replace({"all": "state-based"})

    # Sanity checks
    assert set(tb_ucdp_countries["conflict_type"]) - set(tb_prio_countries["conflict_type"]) == {
        "one-sided violence"
    }, "Missmatch in conflict_type between UCDP and PRIO (country) not as expected!"
    assert set(tb_prio_countries["conflict_type"]) - set(tb_ucdp_countries["conflict_type"]) == {
        "extrasystemic"
    }, "Missmatch in conflict_type between UCDP and PRIO (country) not as expected!"

    # Preserve only pre-UCDP-time data in PRIO
    assert tb_ucdp_countries["year"].min() == YEAR_UCDP_MIN, "UCDP year min is not as expected!"
    tb_prio_countries = tb_prio_countries[tb_prio_countries["year"] < YEAR_UCDP_MIN]

    # Fix extrasystemic: UCDP has no data for extrasystemic, we add zeroes.
    ## Sanity check: no extrasystemic coming from UCDP
    assert "extrasystemic" not in set(tb_ucdp_countries["conflict_type"]), "Extrasystemic conflicts found in UCDP!"
    # Build extrasystemic data for UCDP (all zeroes)
    tb_extra = tb_ucdp_countries[tb_ucdp_countries["conflict_type"] == "interstate"]
    tb_extra["conflict_type"] = "extrasystemic"
    tb_extra["participated_in_conflict"] = 0
    ## Concatenate with og
    tb = pr.concat([tb_ucdp_countries, tb_extra], ignore_index=True)

    # Concatenate
    tb = pr.concat(
        [tb_ucdp_countries, tb_prio_countries], axis=0, ignore_index=True, short_name=f"{paths.short_name}_country"
    )
    return tb
