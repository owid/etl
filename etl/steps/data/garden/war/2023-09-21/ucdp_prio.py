"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from shared import add_indicators_extra

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Index columns
COLUMNS_INDEX = ["year", "region", "conflict_type"]
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
    ds_meadow = paths.load_dataset("ucdp")
    # Read table from meadow dataset.
    tb_ucdp = ds_meadow["ucdp"].reset_index()

    # Load meadow dataset.
    ds_meadow = paths.load_dataset("prio_v31")
    # Read table from meadow dataset.
    tb_prio = ds_meadow["prio_v31"].reset_index()

    # Read table from COW codes
    ds_cow_ssm = paths.load_dataset("gleditsch")
    tb_regions = ds_cow_ssm["gleditsch_regions"].reset_index()

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

    # Set index
    tb = tb.set_index(COLUMNS_INDEX, verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
