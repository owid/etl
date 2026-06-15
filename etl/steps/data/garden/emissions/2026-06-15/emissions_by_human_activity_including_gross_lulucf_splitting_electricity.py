"""GHG emissions by human activity (gross land use), splitting electricity into the consuming activities.

This step loads ``emissions_by_human_activity_including_gross_lulucf`` (gross agriculture-and-land-use
emissions, with electricity as its own activity) and redistributes the electricity emissions into the
activities that consume the electricity, using UNdata's Energy Statistics Database. It is identical in
method to ``emissions_by_human_activity_splitting_electricity``, but operates on the gross-LULUCF tables.

It produces two tables:

1. ``emissions_by_human_activity_including_gross_lulucf_splitting_electricity`` ("Other" folded in).
2. ``emissions_by_human_activity_including_gross_lulucf_including_other_splitting_electricity`` ("Other" shown).
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping of UNdata final-electricity-consumption transactions onto intermediate sectors.
SECTOR_UN_MAPPING = {
    "industry": ["Consumption by manufacturing, construction and non-fuel industry"],
    "agriculture": ["Consumption by agriculture, forestry and fishing"],
    "transport": ["Consumption by transport"],
    "buildings": [
        "Consumption by households",
        "Consumption by commerce and public services",
    ],
    "other": ["Consumption not elsewhere specified (other)"],
}

# List of UNdata sectors.
SECTORS_UN = sum(SECTOR_UN_MAPPING.values(), [])

# Name of UNdata transaction for final energy consumption (used to compute shares).
COLUMN_UN_FINAL_ENERGY = "Final energy consumption"

# Mapping of intermediate sectors onto our activities, when "Other" is shown as its own activity.
ACTIVITY_FROM_INTERMEDIATE = {
    "industry": "Making things",
    "agriculture": "Growing food",
    "transport": "Getting around",
    "buildings": "Keeping warm and cool",
    "other": "Other",
}

# Same, but when there is no separate "Other" activity, its electricity consumption is folded into "Making things".
ACTIVITY_FROM_INTERMEDIATE_BASE = {**ACTIVITY_FROM_INTERMEDIATE, "other": "Making things"}


def sanity_check_un_data(tb_un):
    error = "Unexpected units."
    assert set(tb_un["unit"]) == {"Gigawatt-hours"}, error
    # Ensure the total final energy consumption equals the sum of all sectors.
    tb_un_sum = tb_un[tb_un["sector"].isin(SECTORS_UN)].groupby("year", as_index=False).agg({"value": "sum"})
    tb_un_total = tb_un[tb_un["sector"] == COLUMN_UN_FINAL_ENERGY].groupby("year", as_index=False).agg({"value": "sum"})
    tb_compared = tb_un_sum.merge(tb_un_total, on=["year"], how="inner", suffixes=("", "_sum"))
    error = "Expected UN's final energy consumption to agree with the sum of the electricity consumption of each sector, within 0.01%."
    assert (100 * (abs(tb_compared["value"] - tb_compared["value_sum"]) / tb_compared["value"]) < 0.01).all(), error


def fix_limited_data_coverage(tb_un):
    # The data coverage of the final year in the data is clearly incomplete; one can see that global data drops suddenly.
    tb_un_totals = (
        tb_un[tb_un["sector"] == COLUMN_UN_FINAL_ENERGY]
        .groupby("year", as_index=False)
        .agg({"value": "sum"})
        .sort_values("year")
    )
    latest_value = tb_un_totals["value"].iloc[-1]
    previous_value = tb_un_totals["value"].iloc[-2]
    error = "Expected a sharp drop in the latest year of UN final energy data (likely incomplete coverage)."
    assert latest_value < 0.9 * previous_value, error

    # Drop the latest year in the data.
    tb_un = tb_un[tb_un["year"] < tb_un["year"].max()].reset_index(drop=True)

    return tb_un


def fix_issue_with_switzerland_liechtenstein(countries, tb_un):
    # UNdata has data for Switzerland-Liechtenstein, as well as for Liechtenstein.
    # However, the latter country only has data for sector "other", and the full electricity share goes there.
    error = "Expected Switzerland to be missing in UNdata (since it contains Switzerland-Liechtenstein)."
    assert set(countries) - set(tb_un["country"]) == {"Switzerland"}, error
    error = "Expected Liechtenstein to only have data for 'other' sector, with the full share of electricity."
    liech_nonzero = tb_un[(tb_un["country"] == "Liechtenstein") & (tb_un["electricity_share"] > 0)]
    assert (liech_nonzero["sector"] == "other").all() and (liech_nonzero["electricity_share"] == 1).all(), error
    # I don't understand the details of this split.
    # For the purpose of this step (where we calculate shares of electricity) for now we can simply assume that the shares of final electricity consumption of both countries are the same.
    # So, I'll drop Liechtenstein data, then rename "Switzerland-Liechtenstein" -> "Switzerland", and then repeat Switzerland's data, and assign it to Liechtenstein.
    tb_un.loc[tb_un["country"] == "Switzerland-Liechtenstein", "country"] = "Switzerland"
    tb_un = pr.concat(
        [
            tb_un[tb_un["country"] != "Liechtenstein"],
            tb_un[tb_un["country"] == "Switzerland"].assign(**{"country": "Liechtenstein"}),
        ],
        ignore_index=True,
    )

    return tb_un


def electricity_shares(tb_un, countries):
    # Select relevant UN commodity.
    tb_un = tb_un[(tb_un["commodity"] == "Total Electricity")].drop(columns=["commodity"]).reset_index(drop=True)
    error = "Expected UN energy sectors not found."
    assert set(SECTORS_UN) < set(tb_un["transaction"]), error
    # Select relevant UN transactions.
    tb_un = (
        tb_un[(tb_un["transaction"].isin([COLUMN_UN_FINAL_ENERGY] + SECTORS_UN))]
        .rename(columns={"transaction": "sector"}, errors="raise")
        .reset_index(drop=True)
    )

    # Sanity checks.
    sanity_check_un_data(tb_un=tb_un)

    # For convenience, adapt units, from GWh to TWh.
    tb_un["value"] *= 1e-3
    tb_un = tb_un.drop(columns=["unit"], errors="raise")

    # Fix limited data coverage.
    tb_un = fix_limited_data_coverage(tb_un=tb_un)

    # Create shares of final electricity consumption by intermediate sector.
    tb_un = tb_un.pivot(index=["country", "year"], columns=["sector"], join_column_levels_with="_")
    tb_un = tb_un.rename(columns={column: column.replace("value_", "") for column in tb_un.columns}, errors="raise")
    tb_un = tb_un.rename(columns={COLUMN_UN_FINAL_ENERGY: "total"}, errors="raise")
    for sector, subsectors in SECTOR_UN_MAPPING.items():
        tb_un[sector] = tb_un[subsectors].sum(axis=1) / tb_un["total"]
    tb_un = tb_un[["country", "year"] + list(SECTOR_UN_MAPPING)]

    # For convenience, transpose table.
    tb_un = tb_un.melt(id_vars=["country", "year"], value_name="electricity_share", var_name="sector")

    # Fix known issue with Switzerland-Liechtenstein.
    tb_un = fix_issue_with_switzerland_liechtenstein(countries=countries, tb_un=tb_un)

    return tb_un


def shares_by_activity(tb_shares, mapping):
    # Re-aggregate intermediate-sector shares onto our activities.
    tb = tb_shares.copy()
    tb["sector"] = tb["sector"].map(mapping)
    tb = tb.groupby(["country", "year", "sector"], as_index=False, observed=True).agg({"electricity_share": "sum"})
    return tb


def split_electricity(tb_step1, tb_shares):
    # Total electricity-generation emissions per country and year (the "Electricity" activity).
    elec = (
        tb_step1[tb_step1["sector"] == "Electricity"][["country", "year", "ghg_emissions"]]
        .rename(columns={"ghg_emissions": "electricity_total"}, errors="raise")
        .reset_index(drop=True)
    )

    # Direct emissions of every activity except electricity.
    tb = tb_step1[tb_step1["sector"] != "Electricity"].rename(
        columns={"ghg_emissions": "ghg_emissions_direct"}, errors="raise"
    )
    tb = tb.drop(columns=[c for c in ["co2_emissions"] if c in tb.columns])

    # Attach electricity-consumption shares and total electricity emissions (only countries with UN data survive).
    tb = tb.merge(tb_shares, on=["country", "year", "sector"], how="inner")
    tb = tb.merge(elec, on=["country", "year"], how="inner")

    # Indirect emissions = share of electricity consumed by the activity, times total electricity emissions.
    tb["ghg_emissions_indirect"] = tb["electricity_share"] * tb["electricity_total"]
    tb["ghg_emissions"] = tb["ghg_emissions_direct"] + tb["ghg_emissions_indirect"]
    tb = tb.drop(columns=["electricity_share", "electricity_total"], errors="raise")

    return tb


def sanity_check_outputs(tb, activities):
    error = "Unexpected set of activities in the output."
    assert set(tb["sector"]) == set(activities), error
    error = "Output has missing GHG emissions."
    assert tb[["ghg_emissions", "ghg_emissions_direct", "ghg_emissions_indirect"]].notna().all().all(), error
    error = "Negative indirect emissions found."
    assert (tb["ghg_emissions_indirect"] >= 0).all(), error


def run() -> None:
    #
    # Load inputs.
    #
    # Load the gross-LULUCF step (electricity as its own activity) and read both tables.
    ds_step1 = paths.load_dataset("emissions_by_human_activity_including_gross_lulucf")
    tb_base = ds_step1.read("emissions_by_human_activity_including_gross_lulucf")
    tb_other = ds_step1.read("emissions_by_human_activity_including_gross_lulucf_including_other")

    # Load UN energy statistics database.
    ds_un = paths.load_dataset("energy_statistics_database")
    tb_un = ds_un.read("energy_statistics_database")

    #
    # Process data.
    #
    # Compute shares of final electricity consumption by intermediate sector.
    tb_shares = electricity_shares(tb_un=tb_un, countries=set(tb_base["country"]))

    # Split electricity into the activities that consume it, for each variant.
    tb = split_electricity(tb_base, shares_by_activity(tb_shares, ACTIVITY_FROM_INTERMEDIATE_BASE))
    tb_other = split_electricity(tb_other, shares_by_activity(tb_shares, ACTIVITY_FROM_INTERMEDIATE))

    # Sanity checks.
    sanity_check_outputs(tb, activities=[a for a in tb_base["sector"].unique() if a != "Electricity"])
    sanity_check_outputs(tb_other, activities=[a for a in tb_other["sector"].unique() if a != "Electricity"])

    # Improve table format.
    tb = tb.format(
        keys=["country", "year", "sector"],
        short_name="emissions_by_human_activity_including_gross_lulucf_splitting_electricity",
    )
    tb_other = tb_other.format(
        keys=["country", "year", "sector"],
        short_name="emissions_by_human_activity_including_gross_lulucf_including_other_splitting_electricity",
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_other])
    ds_garden.save()
