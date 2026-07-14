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

# End-target types the Net Zero Tracker treats as "net zero (or similar)" (per its README / methodology).
# On the tracker's website these map to the "Net zero (or similar)" target type. Any other target that
# is not "No target" (e.g. a plain emissions-reduction NDC) is shown by the tracker as an "Other target",
# which is NOT a net-zero commitment.
NET_ZERO_TARGET_TYPES = {
    "Net zero",
    "Zero carbon",
    "Climate neutral",
    "Carbon neutral(ity)",
    "GHG neutral(ity)",
    "Carbon negative",
    "Net negative",
}
# Value the source uses in the end_target column for countries assessed as having no target.
NO_TARGET_END_TARGET = "No target"

# Category labels used in the output indicators (mirroring the tracker's three target-type groups).
NET_ZERO_TARGET_LABEL = "Net-zero (or similar) target"
OTHER_TARGET_LABEL = "Other target"
NO_TARGET_LABEL = "No target"

# Net-zero target statuses defined in the Net Zero Tracker codebook, plus the two labels we assign to
# countries without a net-zero target, so every country the tracker covers has a value on the status
# map (and "no target" / "other target" are distinguishable from "no data").
EXPECTED_STATUSES = {
    "Achieved (externally validated)",
    "Achieved (self-declared)",
    "In law",
    "In policy document",
    "Declaration / pledge",
    "Proposed / in discussion",
    OTHER_TARGET_LABEL,
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

    # Classify each country by the type of end target the tracker records, mirroring the three
    # target-type groups shown on the Net Zero Tracker website:
    #   - a net-zero (or similar) target,
    #   - some other emissions target (e.g. a plain emissions-reduction NDC), which is NOT net zero,
    #   - no target at all (e.g. the United States).
    # Keeping all three lets "other target" and "no target" be distinguished from "no data" (countries
    # the tracker does not cover, which show as missing on charts).
    has_net_zero = tb["end_target"].isin(NET_ZERO_TARGET_TYPES)
    no_target = tb["end_target"] == NO_TARGET_END_TARGET
    other_target = ~has_net_zero & ~no_target

    # Whether the country has set a net-zero target (three-way, covering every country the tracker tracks).
    tb["has_net_zero_target"] = pd.Series(pd.NA, index=tb.index, dtype="string")
    tb.loc[has_net_zero, "has_net_zero_target"] = NET_ZERO_TARGET_LABEL
    tb.loc[other_target, "has_net_zero_target"] = OTHER_TARGET_LABEL
    tb.loc[no_target, "has_net_zero_target"] = NO_TARGET_LABEL
    tb["has_net_zero_target"] = tb["has_net_zero_target"].copy_metadata(tb["net_zero_status"])

    # Status of the net-zero target. For net-zero-target countries this is the recorded status; for the
    # others we show why there is no status ("Other target" or "No target"), so a country the tracker has
    # assessed never appears as "no data".
    status_metadata = tb["net_zero_status"].metadata
    tb["net_zero_status"] = tb["net_zero_status"].astype("string")
    tb.loc[other_target, "net_zero_status"] = OTHER_TARGET_LABEL
    tb.loc[no_target, "net_zero_status"] = NO_TARGET_LABEL
    tb["net_zero_status"].metadata = status_metadata

    # The target year is shown only for countries with a net-zero target.
    tb.loc[~has_net_zero, "net_zero_target_year"] = pd.NA
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
    # Every country the tracker covers must have a status value (a net-zero status, or "Other target" /
    # "No target"); none should silently drop off the status map as "no data".
    assert tb["net_zero_status"].notna().all(), "A country has no net_zero_status (a net-zero target with a blank status?)."
    unexpected = set(tb["net_zero_status"]) - EXPECTED_STATUSES
    assert not unexpected, f"Unexpected net-zero status values: {unexpected}"
    # has_net_zero_target is the three-way target-type classification, covering every country.
    assert set(tb["has_net_zero_target"]) == {NET_ZERO_TARGET_LABEL, OTHER_TARGET_LABEL, NO_TARGET_LABEL}, (
        "Unexpected has_net_zero_target values."
    )
    # The two categorical indicators must agree on which countries have a net-zero target.
    is_net_zero_status = ~tb["net_zero_status"].isin([OTHER_TARGET_LABEL, NO_TARGET_LABEL])
    is_net_zero_type = tb["has_net_zero_target"] == NET_ZERO_TARGET_LABEL
    assert (is_net_zero_status == is_net_zero_type).all(), (
        "net_zero_status and has_net_zero_target disagree on which countries have a net-zero target."
    )
    # A target year should be present only for net-zero-target countries, and be plausible.
    assert tb.loc[~is_net_zero_type, "net_zero_target_year"].isna().all(), (
        "A country without a net-zero target has a target year."
    )
    years = tb["net_zero_target_year"].dropna()
    assert years.between(2000, 2100).all(), "Target year outside the plausible 2000-2100 range."
    # Coverage should not collapse (a sudden drop signals a parsing/mapping regression).
    n_countries = tb["country"].nunique()
    assert n_countries >= 150, f"Only {n_countries} countries; possible parsing/mapping regression."
