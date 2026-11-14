"""Combine finalized annual and preliminary recent measles cases by US state."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SOURCE_ANNUAL = "CDC WONDER - https://wonder.cdc.gov/nndss-annual-summary.html"
SOURCE_RECENT = "CDC Measles Cases and Outbreaks (preliminary) - https://www.cdc.gov/measles/data-research/index.html"
# Reporting areas that are neither the US total nor a state, DC or New York City.
NON_STATE_AREAS = {
    "New England",
    "Middle Atlantic",
    "East North Central",
    "West North Central",
    "South Atlantic",
    "East South Central",
    "West South Central",
    "Mountain",
    "Pacific",
    "Territories",
    "American Samoa",
    "Commonwealth of Northern Mariana Islands",
    "Guam",
    "Puerto Rico",
    "U.S. Virgin Islands",
    "Non-U.S. Residents",
}
# 50 states (with New York State excluding New York City), the District of Columbia and New York City.
N_STATE_AREAS = 52


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("measles_state_level")
    tb_annual = ds_meadow.read("measles_state_level")
    tb_recent = ds_meadow.read("measles_cases_by_state")

    #
    # Process data.
    #
    # Prepare the finalized annual counts.
    tb_annual["disease"] = tb_annual["disease"].str.replace("Measles, ", "").copy_metadata(tb_annual["disease"])
    tb_annual = tb_annual[~tb_annual["country"].isin(NON_STATE_AREAS)]
    # WONDER writes counts with thousands separators (e.g. "1,192").
    tb_annual["case_count"] = pr.to_numeric(
        tb_annual["case_count"].astype(str).str.replace(",", "").copy_metadata(tb_annual["case_count"])
    )
    tb_annual["source"] = SOURCE_ANNUAL
    last_annual_year = tb_annual["year"].max()

    # Add preliminary counts for the complete years since the last finalized one.
    tb_recent = recent_counts(tb_recent, after_year=last_annual_year)
    tb_recent["source"] = SOURCE_RECENT

    tb = pr.concat([tb_annual, tb_recent], ignore_index=True)
    tb["source"] = tb["source"].copy_metadata(tb["case_count"])
    check_reporting_areas(tb, recent_years=set(tb_recent["year"]))

    # Separate the national data into a separate table.
    msk = tb["country"] == "United States"
    tb_usa = tb[msk]
    tb = tb[~msk]
    check_states_add_up_to_national(tb, tb_usa)

    tb = tb.format(["country", "disease", "year"], short_name="state_measles")
    tb_usa = tb_usa.format(["country", "disease", "year"], short_name="national_measles")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_usa], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def recent_counts(tb: Table, after_year: int) -> Table:
    """Preliminary counts by state for the complete years after `after_year`, with their sum as the US total.

    The page only gives total cases (not split into imported and indigenous), and its US total includes international
    visitors, so we add up the states instead, to match the annual data's US residents total.
    """
    # Skip the year the page was last updated in, which is still in progress.
    current_year = pr.to_datetime(tb["last_updated"]).dt.year.max()
    tb = tb[(tb["year"] > after_year) & (tb["year"] < current_year)]
    assert tb["year"].nunique() > 0, f"No complete year of preliminary data after {after_year}."

    # The page reports New York City separately from the rest of New York State, as the annual data does.
    tb["country"] = tb["geography"].replace({"New York": "New York (excluding New York City)"})
    tb["case_count"] = pr.to_numeric(tb["cases_calendar_year"])
    tb["disease"] = "Total"
    tb = tb[["country", "year", "disease", "case_count"]]

    tb_usa = tb.groupby(["year", "disease"], as_index=False)["case_count"].sum()
    tb_usa["country"] = "United States"
    tb = pr.concat([tb, tb_usa], ignore_index=True)

    return tb


def check_reporting_areas(tb: Table, recent_years: set[int]) -> None:
    """Each year and disease should have the US total plus exactly one row for each state, DC and New York City."""
    counts = tb.groupby(["year", "disease"])["country"].agg(["nunique", "size"])
    assert (counts["nunique"] == counts["size"]).all(), "Duplicate reporting areas."
    assert (counts["size"] == N_STATE_AREAS + 1).all(), f"Unexpected reporting areas:\n{counts}"
    # Finalized years are split into imported and indigenous cases; preliminary years only have the total.
    diseases = tb.groupby("year")["disease"].agg(set)
    for year, found in diseases.items():
        expected = {"Total"} if year in recent_years else {"Imported", "Indigenous", "Total"}
        assert found == expected, f"Unexpected diseases in {year}: {found}"


def check_states_add_up_to_national(tb: Table, tb_usa: Table) -> None:
    states = tb.groupby(["year", "disease"])["case_count"].sum()
    national = tb_usa.set_index(["year", "disease"])["case_count"]
    diff = (states - national).abs()
    assert (diff == 0).all(), f"State counts do not add up to the national count:\n{diff[diff != 0]}"
