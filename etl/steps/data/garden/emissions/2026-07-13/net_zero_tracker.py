"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Reference year for the snapshot. The tracker is a rolling database with no time dimension of its own,
# so we place every assessed country at this single "as of" year. The year each country aims to reach
# net-zero is kept separately, in the net_zero_target_year indicator.
AS_OF_YEAR = 2026

# Columns to read from the main table, and how to rename them.
COLUMNS = {
    "name": "country",
    "entity_type": "actor_type",
    "end_target": "end_target",
    "status_of_end_target": "net_zero_status",
    "end_target_year": "net_zero_target_year",
}

# Value the source uses for countries it has assessed as having no net-zero target.
NO_TARGET_LABEL = "No target"

# Countries that have a target but a blank status cell in the source. This is a known provider data
# gap that we leave as missing ("no data") pending confirmation from the producer. The sanity check
# asserts this set is unchanged, so a fix (or a new gap) surfaces at build time for re-investigation.
# NOTE: Chad has a documented emissions-reduction target (19.3% by 2030, in its NDC) but no status.
#  It was reclassified from a "Net zero" pledge ("Proposed / in discussion" in 2023) to an
#  "Emissions reduction target" in 2026, and the status was left blank. Its comparable peers (NDC-based
#  emissions-reduction targets) are almost all "In policy document".
KNOWN_TARGET_WITHOUT_STATUS = {"Chad"}

# Possible net-zero target statuses for countries, as defined in the Net Zero Tracker codebook, plus
# the "No target" label we assign to countries the tracker assessed as having no target.
EXPECTED_STATUSES = {
    "Achieved (externally validated)",
    "Achieved (self-declared)",
    "In law",
    "In policy document",
    "Declaration / pledge",
    "Proposed / in discussion",
    NO_TARGET_LABEL,
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("net_zero_tracker")
    tb = ds_meadow["net_zero_tracker"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Keep only country-level entities (the source also tracks regions, cities and companies).
    tb = tb[tb["actor_type"] == "Country"].drop(columns=["actor_type"]).reset_index(drop=True)

    # Every country the tracker covers is assessed as either having a target or explicitly having none.
    # We keep both, so that "no target" (e.g. the United States) is distinguishable from "no data"
    # (countries the tracker does not cover, which show as missing on charts).
    no_target = tb["end_target"] == NO_TARGET_LABEL

    # Whether the country has set a net-zero target at all.
    tb["has_net_zero_target"] = "Has set a net-zero target"
    tb.loc[no_target, "has_net_zero_target"] = NO_TARGET_LABEL
    tb["has_net_zero_target"] = tb["has_net_zero_target"].copy_metadata(tb["net_zero_status"])

    # The status of the target. Countries assessed as having no target are labelled "No target".
    # Countries that have a target but no status recorded in the source (see KNOWN_TARGET_WITHOUT_STATUS)
    # keep a missing status and show as "no data" on the status map.
    net_zero_status_metadata = tb["net_zero_status"].metadata
    tb["net_zero_status"] = tb["net_zero_status"].astype("string")
    tb.loc[no_target, "net_zero_status"] = NO_TARGET_LABEL
    tb["net_zero_status"].metadata = net_zero_status_metadata

    # The target year only makes sense for countries that have a target.
    tb.loc[no_target, "net_zero_target_year"] = pd.NA
    tb["net_zero_target_year"] = tb["net_zero_target_year"].astype("Int64")

    tb = tb.drop(columns=["end_target"])

    # Place every country at the snapshot's reference year.
    tb["year"] = AS_OF_YEAR

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    sanity_check_outputs(tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    error = "Expected source columns are missing (the producer may have renamed them)."
    assert set(COLUMNS) <= set(tb.columns), error
    assert "Country" in set(tb["entity_type"]), "No country-level entities found in the source."


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.empty, "Output table is empty."
    # Each country should appear only once.
    assert not tb.duplicated(subset=["country"]).any(), "Duplicate country rows."
    # Statuses (where set) must be within the expected set.
    unexpected = set(tb["net_zero_status"].dropna()) - EXPECTED_STATUSES
    assert not unexpected, f"Unexpected net-zero status values: {unexpected}"
    # The only countries left without a status must be the known provider data gap (a target but a
    # blank status cell). If this set changes, the producer may have fixed it or a new gap appeared.
    target_without_status = set(tb.loc[tb["net_zero_status"].isna(), "country"])
    assert target_without_status == KNOWN_TARGET_WITHOUT_STATUS, (
        f"Countries with a target but no status changed from {KNOWN_TARGET_WITHOUT_STATUS} to {target_without_status}; re-investigate."
    )
    # has_net_zero_target is a two-category flag covering every assessed country.
    assert set(tb["has_net_zero_target"]) == {"Has set a net-zero target", NO_TARGET_LABEL}, (
        "Unexpected has_net_zero_target values."
    )
    # "No target" must be consistent between the two categorical indicators.
    assert ((tb["net_zero_status"] == NO_TARGET_LABEL) == (tb["has_net_zero_target"] == NO_TARGET_LABEL)).all(), (
        "Mismatch between 'No target' rows in net_zero_status and has_net_zero_target."
    )
    # Target years, where present, should be plausible.
    years = tb["net_zero_target_year"].dropna()
    assert years.between(2000, 2100).all(), "Target year outside the plausible 2000-2100 range."
    # A "No target" country must not carry a target year.
    assert tb.loc[tb["has_net_zero_target"] == NO_TARGET_LABEL, "net_zero_target_year"].isna().all(), (
        "A 'No target' country has a target year."
    )
    # Coverage should not collapse (a sudden drop signals a parsing/mapping regression).
    n_countries = tb["country"].nunique()
    assert n_countries >= 150, f"Only {n_countries} countries; possible parsing/mapping regression."
