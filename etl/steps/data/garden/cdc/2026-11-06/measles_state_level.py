"""Combine finalized annual and provisional weekly measles cases by US state."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SOURCE_ANNUAL = "CDC WONDER - https://wonder.cdc.gov/nndss-annual-summary.html"
SOURCE_WEEKLY = "CDC NNDSS Weekly Data (provisional) - https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc"
# Reporting areas in the weekly data that are named differently in the annual data.
WEEKLY_TO_ANNUAL_NAMES = {
    "NEW YORK": "New York (excluding New York City)",
    "US RESIDENTS": "United States",
    "U.S. RESIDENTS": "United States",
    "NON-US RESIDENTS": "Non-U.S. Residents",
    "US TERRITORIES": "Territories",
    "U.S. TERRITORIES": "Territories",
    "NORTHERN MARIANA ISLANDS": "Commonwealth of Northern Mariana Islands",
}
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
    # US residents plus territories and non-residents, in the weekly data.
    "Total",
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
    tb_weekly = ds_meadow.read("nndss_weekly_measles")

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

    # Add provisional counts for the complete years since the last finalized one.
    tb_weekly = year_end_counts(tb_weekly, after_year=last_annual_year)
    tb_weekly = tb_weekly[~tb_weekly["country"].isin(NON_STATE_AREAS)]
    tb_weekly["source"] = SOURCE_WEEKLY

    tb = pr.concat([tb_annual, tb_weekly], ignore_index=True)
    tb["source"] = tb["source"].copy_metadata(tb["case_count"])
    check_reporting_areas(tb)

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


def year_end_counts(tb: Table, after_year: int) -> Table:
    """Annual provisional counts by reporting area, for the complete years after `after_year`.

    The weekly data gives each week's cumulative count for the year so far (`m3`), so the year's last week holds the
    count for the whole year. A blank count flagged "-" means no reported cases.
    """
    tb = tb[tb["year"] > after_year]
    last_week = tb.groupby("year")["week"].transform("max")
    tb = tb[tb["week"] == last_week]
    # Skip the current, incomplete year (MMWR years have 52 or 53 weeks).
    tb = tb[tb["week"] >= 52]
    assert tb["year"].nunique() > 0, f"No complete year of weekly data after {after_year}."

    assert tb.loc[tb["m3"].isna(), "m3_flag"].eq("-").all(), "Unexpected blank counts in the weekly data."
    tb["m3"] = tb["m3"].fillna(0)

    tb["country"] = tb["states"].astype(str).str.upper().replace(WEEKLY_TO_ANNUAL_NAMES)
    # Match the annual data's capitalization (e.g. "DISTRICT OF COLUMBIA" -> "District of Columbia").
    tb["country"] = tb["country"].where(tb["country"] != tb["country"].str.upper(), tb["country"].str.title())
    tb["country"] = tb["country"].str.replace(" Of ", " of ").copy_metadata(tb["states"])
    tb["disease"] = tb["label"].str.replace("Measles, ", "").copy_metadata(tb["label"])
    tb = tb.rename(columns={"m3": "case_count"})[["country", "year", "disease", "case_count"]]

    # Add the total (imported plus indigenous) to match the annual data.
    tb_total = tb.groupby(["country", "year"], as_index=False, observed=True)["case_count"].sum()
    tb_total["disease"] = "Total"
    tb = pr.concat([tb, tb_total], ignore_index=True)

    return tb


def check_reporting_areas(tb: Table) -> None:
    """Each year and disease should have the US total plus exactly one row for each state, DC and New York City."""
    counts = tb.groupby(["year", "disease"])["country"].agg(["nunique", "size"])
    assert (counts["nunique"] == counts["size"]).all(), "Duplicate reporting areas."
    assert (counts["size"] == N_STATE_AREAS + 1).all(), f"Unexpected reporting areas:\n{counts}"
    assert set(tb["disease"]) == {"Imported", "Indigenous", "Total"}


def check_states_add_up_to_national(tb: Table, tb_usa: Table) -> None:
    states = tb.groupby(["year", "disease"])["case_count"].sum()
    national = tb_usa.set_index(["year", "disease"])["case_count"]
    diff = (states - national).abs()
    assert (diff == 0).all(), f"State counts do not add up to the national count:\n{diff[diff != 0]}"
