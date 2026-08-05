"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the main table, and how to rename them. end_target is read only to identify
# countries assessed as having no target.
COLUMNS = {
    "name": "country",
    "end_target_year": "year",
    "status_of_end_target": "net_zero_status",
    "entity_type": "actor_type",
    "end_target": "end_target",
}

# Value the source uses in the end_target column for countries assessed as having no target.
NO_TARGET_END_TARGET = "No target"
# Label shown for those countries (distinct from "no data" = countries the tracker does not cover).
NO_TARGET_LABEL = "No target"

# Possible net-zero target statuses in the Net Zero Tracker codebook, plus the "No target" label.
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

    # Select only rows that correspond to countries.
    tb = tb[tb["actor_type"] == "Country"].drop(columns=["actor_type"]).reset_index(drop=True)

    # Keep countries with both a target status and a target year, plus countries the tracker explicitly
    # assessed as having no target. Keeping the latter lets "no target" be distinguished from "no data"
    # (countries the tracker does not cover, which stay missing on charts). Countries with a target but
    # no recorded status are dropped and show as missing data.
    no_target = tb["end_target"] == NO_TARGET_END_TARGET
    keep = (tb["net_zero_status"].notna() & tb["year"].notna()) | no_target

    # NOTE: Chad has an emissions-reduction target (2030) but the tracker records no status for it, so it
    #  is dropped and shows as no data. This matches zerotracker.net, where Chad's "Target Status" row is
    #  blank. It is the only country with a target but no status; this assert trips if that ever changes
    #  (the producer fills it in, or a new country lands in the same situation), prompting a re-check.
    target_without_status = set(tb.loc[~keep, "country"])
    assert target_without_status <= {"Chad"}, (
        f"New country with a target but no recorded status (was only Chad): {target_without_status - {'Chad'}}"
    )

    tb = tb[keep].reset_index(drop=True)
    no_target = tb["end_target"] == NO_TARGET_END_TARGET

    # "No target" countries have no target year; place them at the data's publication year (read from
    # the origin) so they appear on the (timeline-hidden) maps.
    snapshot_year = int(tb["net_zero_status"].metadata.origins[0].date_published[:4])
    tb.loc[no_target, "year"] = snapshot_year
    tb["year"] = tb["year"].astype(int)

    # The source status is categorical, so cast to string to be able to label "no target" countries.
    tb["net_zero_status"] = tb["net_zero_status"].astype("string")
    tb.loc[no_target, "net_zero_status"] = NO_TARGET_LABEL

    tb = tb.drop(columns=["end_target"])

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Flag whether the country has set a net-zero target; countries assessed as having no target are
    # labelled "No target".
    tb["has_net_zero_target"] = "Net-zero achieved or pledged"
    tb.loc[no_target, "has_net_zero_target"] = NO_TARGET_LABEL
    tb["has_net_zero_target"] = tb["has_net_zero_target"].copy_metadata(tb["net_zero_status"])

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
    # Statuses must be within the expected set.
    unexpected = set(tb["net_zero_status"].dropna()) - EXPECTED_STATUSES
    assert not unexpected, f"Unexpected net-zero status values: {unexpected}"
    # has_net_zero_target is a two-category flag.
    assert set(tb["has_net_zero_target"]) == {"Net-zero achieved or pledged", NO_TARGET_LABEL}, (
        "Unexpected has_net_zero_target values."
    )
    # "No target" must be consistent between the two indicators.
    assert ((tb["net_zero_status"] == NO_TARGET_LABEL) == (tb["has_net_zero_target"] == NO_TARGET_LABEL)).all(), (
        "Mismatch between 'No target' rows in net_zero_status and has_net_zero_target."
    )
    # Target years should be plausible.
    assert tb["year"].between(2000, 2100).all(), "Target year outside the plausible 2000-2100 range."
    # Coverage should not collapse (a sudden drop signals a parsing/mapping regression).
    n_countries = tb["country"].nunique()
    assert n_countries >= 150, f"Only {n_countries} countries; possible parsing/mapping regression."
