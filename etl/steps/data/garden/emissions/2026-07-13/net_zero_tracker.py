"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the main table, and how to rename them.
COLUMNS = {
    "name": "country",
    "end_target_year": "year",
    "status_of_end_target": "net_zero_status",
    "entity_type": "actor_type",
}

# Possible net-zero target statuses for countries, as defined in the Net Zero Tracker codebook.
EXPECTED_STATUSES = {
    "Achieved (externally validated)",
    "Achieved (self-declared)",
    "In law",
    "In policy document",
    "Declaration / pledge",
    "Proposed / in discussion",
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

    # Remove rows with incomplete data (no target status and/or no target year).
    # NOTE: Some countries are dropped because they lack a status and/or target year. In this snapshot
    #  these include the United States (whose end target is now "No target"), Bolivia, Chad, Guinea,
    #  Libya and Syria.
    tb = tb.dropna(how="any").reset_index(drop=True)

    # The target year is the year by which the country aims to reach net-zero; store it as an integer.
    tb["year"] = tb["year"].astype(int)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Add a column that simply indicates whether the country has a net-zero target.
    # NOTE: All countries remaining in the table have set a net-zero target. Countries without one are
    #  absent from the table (and show as missing data in charts).
    tb["has_net_zero_target"] = "Net-zero achieved or pledged"
    # Copy metadata from another variable.
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
    # Each country should appear only once (a single net-zero target year).
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    # Statuses must be within the codebook's set of possible values.
    unexpected = set(tb["net_zero_status"]) - EXPECTED_STATUSES
    assert not unexpected, f"Unexpected net-zero status values: {unexpected}"
    # Target years should be plausible.
    assert tb["year"].between(2000, 2100).all(), "Target year outside the plausible 2000-2100 range."
    # The has_net_zero_target flag is a single constant category.
    assert set(tb["has_net_zero_target"]) == {"Net-zero achieved or pledged"}, "Unexpected has_net_zero_target values."
    # Coverage should not collapse (a sudden drop signals a parsing/mapping regression).
    n_countries = tb["country"].nunique()
    assert n_countries >= 150, f"Only {n_countries} countries; possible parsing/mapping regression."
