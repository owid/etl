"""Curate data from the IEA's Critical Minerals Dataset.

Analysis of the Critical Mineral Dataset file:
* Sheet 2 is the only table on supply, which is also the only table with country data.
  NOTE: Region aggregates are not possible, since there is data for "Rest of world" for each mineral.
* Sheets 4.1, ..., 4.6 are called, e.g. "4.1 Solar" (so let's call them 4.x sheets, or individual demand sheets). They contain all relevant data on demand for individual technologies.
* Sheet 3.1 (called "3.1 Cleantech demand by tech") contains a summary of the main minerals used in clean tech listed in the 4.x sheets.
  NOTE: This sheet seems to assume "Base case", even if it doesn't explicitly mention it.
  But sheet 3.1 also contains an additional entity (called "Other low emissions power generation") that corresponds to uses of those minerals for which there is not an individual sheet. They need to be taken into account.
  For example, Chromium only appears in the "4.2 Wind" sheet, with 63.27kt in 2023 (base case). This use is accounted for in sheet 3.1. But in sheet 3.1 we also see "Other low emissions power generation", 148.76kt. So this needs to be taken into account.
* Sheet 3.2 (called "3.2 Cleantech demand by mineral") includes the total uses of minerals in clean tech. It includes minerals that are not in 3.1. So, we can compute the totals from 4.x, and, whatever is left in the 3.2 totals can be assigned to "Other low emissions power generation". Then we can safely ignore sheet 3.1.
  NOTE: This sheet seems to assume "Base case", even if it doesn't explicitly mention it.
  NOTE: There is one mineral that appears in 3.1 and not in 3.2, namely "PGMs". One could think that this is the sum of "Iridium" plus "PGMs (other than iridum)" (also, note that this is misspelled!). However, by looking at the numbers, "PGMs" in 3.1 seems to coincide exactly with "PGMs (other than iridum)".
* Finally, Sheet 1 contains a summary of all demand data from other sheets. However, it also includes uses of minerals from non-clean technologies. This is called "Other uses", and needs to be accounted for. For example, in the case of copper, uses in clean tech amount to 6372kt, and other uses add up to 19543kt.
    NOTE: Sheet 1 contains 2 (aggregate) minerals that are not in sheet 3.2, namely
    - "Magnet rare earth elements". As they mention in the notes of sheet 1, this corresponds to praseodymium (Pr), neodymium (Nd), terbium (Tb) and dysprosium (Dy). Therefore, it should coincide with "Total rare earth elements" from sheet 3.2 (and it roughly does). The easiest solution (to avoid double counting and to be able to include other uses) is to remove the rows for the individual rare earth elements, and keep only "Total rare earth elements" (and rename them as "Magnet rare earth elements").
    - "Graphite (all grades: natural and synthetic)". This is the most problematic case. It shows, for example, 1147kt for EV, whereas, in the EV sheet, we see "Battery-grade graphite" 685.56kt (and no other use of graphite appears). A similar inconsistency happens with grid battery storage. So, it seems that there are additional uses of graphite that are not accounted for in the EV and battery storage sheets. We can include a "Graphite, other than battery-grade" mineral, with the difference.

Therefore, the strategy should be:
* Combine the 4.x sheets into one demand table.
* Assign a "case" column to sheet 3.2 (always assume Base case).
* In demand table, create an aggregate "Magnet rare earth elements" that combines Praseodymium, Neodymium, Terbium and Dysprosium.
* From sheet 3.2, remove rows for Praseodymium, Neodymium, Terbium and Dysprosium, and rename "Total rare earth elements" -> "Magnet rare earth elements". Also, fix the misspelled "iridum".
* Calculate "Other low emissions power generation", which should be the total from 3.2 minus the total from 4.x for each mineral. Add those other low emission uses to the demand table.
* Assign a "case" column to sheet 1.
* Add the other uses of graphite from sheet 1 to the demand table (EV and battery storage).
* Add "Other uses" from sheet 1 to the demand table.
* Sanity check that the totals from the resulting demand table coincides with the totals from sheet 1.

"""

from typing import Tuple

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table, VariablePresentationMeta
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of technologies accounted for in the data.
technologies = {
    "ev": "Electric vehicles",
    "electricity_networks": "Electricity networks",
    "battery_storage": "Grid battery storage",
    "hydrogen": "Hydrogen technologies",
    "solar_pv": "Solar PV",
    "wind": "Wind",
}

# Name of rare earth elements aggregate (as given in the spreadsheet).
RARE_ELEMENTS_LABEL = "Magnet rare earth elements"
RARE_ELEMENTS_LABEL_NEW = "Magnet rare earth elements"

# List of rare element magnets.
RARE_ELEMENTS_LIST = ["Praseodymium", "Neodymium", "Terbium", "Dysprosium"]

# Name of all graphite uses, as listed in sheet 1 (of total demand including uses outside of clean tech).
GRAPHITE_ALL_LABEL = "Graphite (all grades: natural and synthetic)"
# New name.
GRAPHITE_ALL_LABEL_NEW = "Graphite (natural and synthetic)"

# Name of battery-grade graphite, as it appears in the spreadsheet.
GRAPHITE_BATTERY_GRADE_LABEL = "Battery-grade graphite"
# New name.
GRAPHITE_BATTERY_GRADE_LABEL_NEW = "Graphite (battery-grade)"

# Name of a new mineral category, which encompasses the difference in demand between sheet 1 and the combined 4.x sheets.
GRAPHITE_OTHER_LABEL = "Graphite (other than battery-grade)"

# Name of other uses in clean tech.
OTHER_LOW_LABEL = "Other low emissions power generation"

# Name of other uses (in sheet 1).
OTHER_LABEL = "Other uses"

# Name of total demand from clean technologies.
TOTAL_CLEAN_LABEL = "Total clean technologies"

# Name of total demand (in sheet 1).
TOTAL_DEMAND_LABEL = "Total demand"

# New label for "scenario" to use for supply data (that does not depend on scenario).
# NOTE: The new "supply_as_a_share_of_global_demand" column will still depend on scenario, since global demand does.
ALL_SCENARIOS_LABEL = "All scenarios"


def prepare_supply_table(ds_meadow: Dataset) -> Table:
    # Read supply table.
    tb_supply = ds_meadow["supply_for_key_minerals"].reset_index()

    # Ignore aggregates in the data.
    tb_supply = tb_supply[~tb_supply["country"].str.startswith(("Total", "Top"))].reset_index(drop=True)

    # Harmonize country names.
    tb_supply = geo.harmonize_countries(df=tb_supply, countries_file=paths.country_mapping_path)

    # Add a global aggregate for supply.
    tb_supply = pr.concat(
        [
            tb_supply,
            tb_supply.groupby(["year", "mineral", "process", "case"], observed=True, as_index=False)
            .agg({"supply": "sum"})
            .assign(**{"country": "World"}),
        ],
        ignore_index=True,
    )

    return tb_supply


def combine_individual_demand_tables(ds_meadow: Dataset) -> Table:
    # Read, prepare, and concatenate all tables.
    tables = []
    for table_name in [
        "demand_for_battery_storage",
        "demand_for_electricity_networks",
        "demand_for_ev",
        "demand_for_hydrogen",
        "demand_for_solar_pv",
        "demand_for_wind",
    ]:
        technology = technologies[table_name.split("demand_for_")[-1]]
        _tb = ds_meadow[table_name].reset_index().assign(**{"technology": technology})
        tables.append(_tb)
    tb_demand = pr.concat(tables)

    # Ignore aggregates in the data.
    tb_demand = tb_demand[~tb_demand["mineral"].str.startswith(("Total"))].reset_index(drop=True)

    # Combine data for magnet rare earth elements.
    # Check that they are all in the data.
    assert all(
        [(tb_demand["mineral"] == element).any() for element in RARE_ELEMENTS_LIST]
    ), "Missing rare elements in demand table."
    # Calculate total demand of rare elements.
    rare_elements_data = (
        tb_demand[tb_demand["mineral"].isin(RARE_ELEMENTS_LIST)]
        .groupby(["case", "year", "scenario", "technology"], observed=True, as_index=False)
        .agg({"demand": "sum"})
        .assign(**{"mineral": RARE_ELEMENTS_LABEL})
    )
    # Remove individual rare elements and add combined total demand of rare elements.
    tb_demand = pr.concat(
        [tb_demand[~tb_demand["mineral"].isin(RARE_ELEMENTS_LIST)].reset_index(drop=True), rare_elements_data],
        ignore_index=True,
        short_name="demand_for_key_minerals",
    )

    # Fix typo "iridum".
    mapping = {
        "PGMs (other than iridum)": "PGMs (other than iridium)",
    }
    tb_demand["mineral"] = map_series(tb_demand["mineral"], mapping=mapping)

    return tb_demand


def prepare_clean_technologies_by_mineral_table(ds_meadow: Dataset) -> Table:
    # Read table of total clean technology uses by mineral.
    tb_clean = ds_meadow["demand_for_clean_energy_technologies_by_mineral"].reset_index()
    # Even if not explicitly mentioned, it seems that "Base case" is assumed in this sheet.
    tb_clean["case"] = "Base case"
    # Check that they are all in the data.
    assert all(
        [(tb_clean["mineral"] == element).any() for element in RARE_ELEMENTS_LIST]
    ), "Missing rare elements in clean tech by mineral demand table."
    # Remove rows for individual rare earth elements.
    tb_clean = tb_clean[~tb_clean["mineral"].isin(RARE_ELEMENTS_LIST)].reset_index(drop=True)
    # Rename "Total rare earth elements" -> "Magnet rare earth elements".
    # Also, fix typo "iridum".
    mapping = {
        "Total rare earth elements": RARE_ELEMENTS_LABEL,
        "PGMs (other than iridum)": "PGMs (other than iridium)",
    }
    tb_clean["mineral"] = map_series(tb_clean["mineral"], mapping=mapping)

    # Remove row with total demand for minerals.
    tb_clean = tb_clean[~tb_clean["mineral"].str.startswith("Total")].reset_index(drop=True)

    return tb_clean


def add_demand_from_other_clean(tb_demand: Table, tb_clean: Table) -> Table:
    # Calculate "Other low emissions power generation", which should be the total from sheet 3.2 minus the total from sheets 4.x for each mineral. Add those other low emission uses to the demand table.
    tb_demand_total = tb_demand.groupby(["case", "scenario", "mineral", "year"], observed=True, as_index=False).agg(
        {"demand": "sum"}
    )
    combined = tb_demand_total.merge(
        tb_clean, how="inner", on=["case", "scenario", "mineral", "year"], suffixes=("", "_summary")
    )
    # Check that the totals in sheet 3.2 are always larger than the totals calculated from combining the 4.x sheets (or within 1%).
    error = "Sheet on clean technology demand by mineral should be >= the sum of the demands from the 4.x sheets."
    assert combined[(combined["demand_summary"] < (combined["demand"] * 0.99))].empty, error
    combined["difference"] = combined["demand_summary"] - combined["demand"]
    # Create a new technology that contains all the remaining demand not included in the 4.x sheets.
    # NOTE: Only include it if the difference is larger than 1%.
    tb_other_low = (
        combined[combined["difference"] >= (combined["demand"] * 0.01)]
        .assign(**{"technology": OTHER_LOW_LABEL})
        .drop(columns=["demand", "demand_summary"])
        .rename(columns={"difference": "demand"})
        .reset_index(drop=True)
    )

    # Append the demand from other low emissions power generation to the demand table.
    tb_demand_with_other = pr.concat([tb_demand, tb_other_low], ignore_index=True)

    return tb_demand_with_other


def add_demand_from_other_graphite(tb_demand: Table, tb_total: Table) -> Table:
    # Calculate "Graphite, other uses", which should be the total from sheet 1 minus the total from sheets 4.x for each mineral. Add those other uses to the demand table.

    # List all graphite-related minerals listed in the 4.x sheets.
    graphite_uses = [GRAPHITE_BATTERY_GRADE_LABEL]

    # Sanity checks.
    error = "There are new unexpected uses of graphite in the 4.x sheets."
    assert set(tb_demand[tb_demand["mineral"].str.lower().str.contains("graphite")]["mineral"]) == set(
        graphite_uses
    ), error
    error = "There are new unexpected uses of graphite in Sheet 1."
    assert set(tb_total[tb_total["mineral"].str.lower().str.contains("graphite")]["mineral"]) == set(
        [GRAPHITE_ALL_LABEL]
    ), error

    # Calculate the total graphite demand in the total demand sheet.
    tb_graphite_total = (
        tb_total[tb_total["mineral"] == GRAPHITE_ALL_LABEL].drop(columns=["mineral"]).reset_index(drop=True)
    )

    # For now we only want to add the "other graphite uses" to each individual technology.
    # But the "Other uses" will be added to all minerals later.
    # Hence, remove other uses and total demand.
    tb_graphite_total = tb_graphite_total[
        ~tb_graphite_total["technology"].isin([TOTAL_DEMAND_LABEL, OTHER_LABEL])
    ].reset_index(drop=True)

    # Calculate the total graphite demand in the 4.x sheets.
    tb_graphite_demand = (
        tb_demand[tb_demand["mineral"].isin(graphite_uses)].drop(columns=["mineral"]).reset_index(drop=True)
    )
    combined = tb_graphite_demand.merge(
        tb_graphite_total, how="inner", on=["case", "scenario", "technology", "year"], suffixes=("", "_summary")
    )
    # Check that the totals in sheet 1 are always larger than the totals calculated from combining the 4.x sheets (or within 1%).
    error = "Sheet on clean technology graphite demand by mineral should be >= the sum of the graphite demands from the 4.x sheets."
    assert combined[(combined["demand_summary"] < (combined["demand"] * 0.99))].empty, error
    combined["difference"] = combined["demand_summary"] - combined["demand"]

    # Create a new technology that contains all the remaining graphite demand not included in the 4.x sheets.
    # NOTE: Only include it if the difference is larger than 1%.
    tb_graphite_other = (
        combined[combined["difference"] >= combined["demand"] * 0.01]
        .assign(**{"mineral": GRAPHITE_OTHER_LABEL})
        .drop(columns=["demand", "demand_summary"])
        .rename(columns={"difference": "demand"})
        .reset_index(drop=True)
    )

    # Append the demand from other low emissions power generation to the demand table.
    tb_demand_with_graphite_other = pr.concat([tb_demand, tb_graphite_other], ignore_index=True)

    return tb_demand_with_graphite_other


def prepare_total_demand_table(ds_meadow: Dataset) -> Table:
    # Read table on total demand of minerals (including uses outside of clean tech).
    tb_total = ds_meadow["demand_for_key_minerals"].reset_index()

    # Sanity checks.
    assert TOTAL_CLEAN_LABEL in set(tb_total["technology"]), f"'{TOTAL_CLEAN_LABEL}' not found in sheet 1."
    assert not tb_total[
        (tb_total["technology"].str.startswith("Share of"))
    ].empty, "Rows for 'Share of...' not found in sheet 1."

    # Remove rows of shares and total clean technology demand (but keep grand total).
    tb_total = tb_total[
        (tb_total["technology"] != TOTAL_CLEAN_LABEL) & (~tb_total["technology"].str.startswith("Share of"))
    ].reset_index(drop=True)

    # Even if not explicitly mentioned, it seems that "Base case" is assumed in this sheet.
    tb_total["case"] = "Base case"

    return tb_total


def add_demand_from_other_uses(tb_demand: Table, tb_total: Table) -> Table:
    # Add "Other uses" from sheet 1 to the demand table.
    error = f"Technology category '{OTHER_LABEL}' not found in sheet 1."
    assert OTHER_LABEL in set(tb_total[tb_total["technology"].str.lower().str.contains("other")]["technology"]), error

    tb_demand_with_other_uses = pr.concat(
        [tb_demand, tb_total[tb_total["technology"] == OTHER_LABEL].reset_index(drop=True)], ignore_index=True
    )

    return tb_demand_with_other_uses


def run_sanity_checks_on_outputs(tb_demand: Table, tb_total: Table) -> None:
    tb_total_expected = tb_demand.copy()
    # Rename all graphite cases to simply "Graphite".
    all_graphite = tb_total_expected[tb_total_expected["mineral"].str.lower().str.contains("graphite")][
        "mineral"
    ].unique()
    tb_total_expected["mineral"] = map_series(
        tb_total_expected["mineral"], {graphite_case: "Graphite" for graphite_case in all_graphite}
    )

    # Check that the totals from the resulting demand table coincides with the totals from sheet 1.
    tb_total_expected = tb_total_expected.groupby(
        ["case", "scenario", "mineral", "year"], observed=True, as_index=False
    ).agg({"demand": "sum"})

    assert TOTAL_DEMAND_LABEL in set(tb_total["technology"]), f"Technology '{TOTAL_DEMAND_LABEL}' not found in sheet 1."
    tb_total_given = tb_total[tb_total["technology"] == TOTAL_DEMAND_LABEL].reset_index(drop=True)

    # Idem in the total table.
    tb_total_given["mineral"] = map_series(tb_total_given["mineral"], {GRAPHITE_ALL_LABEL: "Graphite"})

    compared = pr.merge(
        tb_total_expected,
        tb_total_given,
        how="inner",
        on=["case", "scenario", "mineral", "year"],
        suffixes=("_expected", "_given"),
    )
    compared["_deviation"] = (
        100 * abs(compared["demand_expected"] - compared["demand_given"]) / compared["demand_given"]
    )

    error = "Larger than 1% discrepancy between the total expected demand from sheets 4.x and the ones from sheet 1."
    assert compared[compared["_deviation"] > 1].empty, error


def add_share_columns(tb_demand: Table, tb_supply: Table) -> Tuple[Table, Table]:
    # Create a table for global demand.
    # For each case-scenario-mineral-year, we need the global demand of all technologies.
    # NOTE: Global mineral demand, including uses outside clean tech, is only given for "Base case" for a few minerals (for which "Other uses" is explicitly given).
    minerals_with_total_demand = tb_demand[tb_demand["technology"] == "Other uses"]["mineral"].unique().tolist()
    tb_demand_global = (
        tb_demand[(tb_demand["case"] == "Base case") & (tb_demand["mineral"].isin(minerals_with_total_demand))]
        .groupby(["case", "scenario", "mineral", "process", "year"], observed=True, as_index=False)
        .agg({"demand": "sum"})
        .rename(columns={"demand": "global_demand"})
    )

    # Create a table for global supply.
    tb_supply_global = (
        tb_supply[tb_supply["country"] == "World"]
        .drop(columns=["country"], errors="raise")
        .rename(columns={"supply": "global_supply"})
    )

    # Add global supply.
    tb_supply = tb_supply.merge(tb_supply_global, on=["case", "year", "mineral", "process"], how="left")
    assert set(tb_supply[tb_supply["supply"] == tb_supply["global_supply"]]["country"]) == set(["World"])

    # We assume that all demand is of refined minerals.
    tb_supply = tb_supply.merge(tb_demand_global, on=["case", "year", "mineral", "process"], how="left")

    # Add "share" columns to supply table.
    tb_supply["supply_as_a_share_of_global_demand"] = 100 * tb_supply["supply"] / tb_supply["global_demand"]
    tb_supply["supply_as_a_share_of_global_supply"] = 100 * tb_supply["supply"] / tb_supply["global_supply"]

    # Drop unnecessary columns.
    tb_supply = tb_supply.drop(columns=["global_demand", "global_supply"], errors="raise")

    # After merging with global demand, "scenario" is added to the supply table.
    # Supply does not change depending on the scenario, but demand does, and therefore supply as a share of global demand changes.
    # As a sanity check, ensure that each row for supply and supply_as_a_share_of_global_supply is repeated 3 times.
    # After flattening the table, remove all unnecessary columns.
    # NOTE: This will happen only for "Refinery" (that was merged with global demand), not for "Mine".
    # NOTE: Ideally, we should not show the scenario dropdown for total supply. But if we replace their scenarios with something else, e.g. "All scenarios", then one can't easily switch between total and share metrics.
    ####################################################################################################################
    # TODO: Again, there are issues with Graphite. Once fixed remove the condition (tb_supply["mineral"] != "Graphite").
    ####################################################################################################################
    grouped = (
        tb_supply[(tb_supply["mineral"] != "Graphite") & (tb_supply["process"] == "Refinery")]
        .groupby(
            [column for column in tb_supply.columns if column not in ["scenario", "supply_as_a_share_of_global_demand"]]
        )
        .size()
    )
    assert (grouped == 3).all(), "Unexpected number of rows (see explanation in the code)."
    # Fill all cases where "scenario" is now nan (for "Mine" process) with a reasonable label.
    tb_supply["scenario"] = tb_supply["scenario"].astype("string").fillna(ALL_SCENARIOS_LABEL)

    # From the original supply table, we need the global supply of each mineral-year (given only for the "Base case").

    # Add total demand to the demand table.
    tb_demand = tb_demand.merge(tb_demand_global, on=["case", "scenario", "mineral", "process", "year"], how="left")
    # TODO: For "Graphite (all grades: natural and synthetic)", "Other uses" has all the demand. There may be a bug.
    #  t[t["demand"]==t["global_demand"]]
    # TODO: After fixing that, consider renaming to "Graphite" and add a subtitle or footnote.

    # Add total supply to the demand table.
    tb_demand = tb_demand.merge(tb_supply_global, on=["case", "year", "mineral", "process"], how="left")

    # Add "share" columns to demand table.
    tb_demand["demand_as_a_share_of_global_demand"] = 100 * tb_demand["demand"] / tb_demand["global_demand"]
    tb_demand["demand_as_a_share_of_global_supply"] = 100 * tb_demand["demand"] / tb_demand["global_supply"]

    # Drop unnecessary columns.
    tb_demand = tb_demand.drop(columns=["global_demand", "global_supply"], errors="raise")

    return tb_demand, tb_supply


def create_demand_by_technology_flat(tb_demand: Table) -> Table:
    # Create a wide-format table.
    tb_demand_by_technology_flat = tb_demand.pivot(
        index=["technology", "year"],
        columns=["mineral", "process", "case", "scenario"],
        values=["demand", "demand_as_a_share_of_global_demand", "demand_as_a_share_of_global_supply"],
        join_column_levels_with="|",
    )

    # Remove empty columns.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.dropna(axis=1, how="all")

    return tb_demand_by_technology_flat


def create_supply_by_country_flat(tb_supply: Table) -> Table:
    # Create a wide-format table.
    tb_supply_flat = tb_supply.pivot(
        index=["country", "year"],
        columns=["mineral", "process", "case", "scenario"],
        values=["supply", "supply_as_a_share_of_global_demand", "supply_as_a_share_of_global_supply"],
        join_column_levels_with="|",
    )

    # Remove empty columns.
    tb_supply_flat = tb_supply_flat.dropna(axis=1, how="all")

    # The "scenario" is only relevant for demand. So, for supply (and supply as a share of global supply) the scenario
    # should always be generic (e.g. "All scenarios"). The only time when we need "scenario" is for
    # "supply_as_a_share_of_global_demand", since global demand does depend on scenario.
    # Therefore, for supply and supply as a share of global supply, ensure there is only one scenario.
    for column in tb_supply_flat.columns:
        scenario = column.split("|")[-1]
        if (scenario != ALL_SCENARIOS_LABEL) and column.startswith(("supply|", "supply_as_a_share_of_global_supply|")):
            column_new = column.replace(scenario, ALL_SCENARIOS_LABEL)
            if column_new in tb_supply_flat.columns:
                tb_supply_flat = tb_supply_flat.drop(columns=[column], errors="raise")
            else:
                tb_supply_flat = tb_supply_flat.rename(columns={column: column_new}, errors="raise")

    # Check that the previous operation was successful.
    for column in tb_supply_flat.drop(columns=["country", "year"]).columns:
        if not column.startswith("supply_as_a_share_of_global_demand"):
            assert column.split("|")[-1] == ALL_SCENARIOS_LABEL

    return tb_supply_flat


def create_demand_by_scenario_flat(tb_demand: Table) -> Table:
    # Create other flat tables for different clean technologies (assuming base case).
    tb_demand_by_scenario_flat = tb_demand[(tb_demand["case"] == "Base case")].pivot(
        index=["scenario", "year"], columns=["mineral", "technology"], values=["demand"], join_column_levels_with="|"
    )

    # Remove empty columns, if any.
    tb_demand_by_scenario_flat = tb_demand_by_scenario_flat.dropna(axis=1, how="all")

    return tb_demand_by_scenario_flat


def clean_demand_table(tb_demand: Table) -> Table:
    # Drop rows with no demand data.
    tb_demand = tb_demand.dropna(subset="demand").reset_index(drop=True)
    # We want the current year to appear in all scenarios (and therefore drop "Current" scenario).
    tb_demand_current = tb_demand[tb_demand["scenario"] == "Current"].reset_index(drop=True)
    tb_demand = tb_demand[tb_demand["scenario"] != "Current"].reset_index(drop=True)
    for scenario in sorted(set(tb_demand[tb_demand["scenario"] != "Current"]["scenario"])):
        tb_demand = pr.concat([tb_demand, tb_demand_current.assign(**{"scenario": scenario})], ignore_index=True)
    # After doing this, some combinations for which there was no data for a given scenario will have only data for 2023.
    # Drop these cases, where there is only one row (one year) for a given case-mineral-scenario-technology combination.
    tb_demand = tb_demand[
        tb_demand.groupby(["case", "mineral", "scenario", "technology"], observed=True, as_index=False)[
            "year"
        ].transform("nunique")
        > 1
    ].reset_index(drop=True)

    # We assume that the demand is only on refined minerals.
    tb_demand["process"] = "Refining"

    return tb_demand


def harmonize_units(tb_demand: Table, tb_supply: Table, tb_total: Table) -> Tuple[Table, Table, Table]:
    tb_demand = tb_demand.copy()
    tb_supply = tb_supply.copy()
    tb_total = tb_total.copy()

    # Convert units from kilotonnes to tonnes.
    tb_demand["demand"] *= 1000
    tb_total["demand"] *= 1000
    tb_supply["supply"] *= 1000

    return tb_demand, tb_supply, tb_total


def harmonize_minerals_and_processes(tb_demand: Table, tb_supply: Table) -> Tuple[Table, Table]:
    # For consistency, rename some minerals.
    tb_demand["mineral"] = tb_demand["mineral"].replace({GRAPHITE_ALL_LABEL: GRAPHITE_ALL_LABEL_NEW})
    tb_demand["mineral"] = tb_demand["mineral"].replace(
        {GRAPHITE_BATTERY_GRADE_LABEL: GRAPHITE_BATTERY_GRADE_LABEL_NEW}
    )
    tb_demand["mineral"] = tb_demand["mineral"].replace({RARE_ELEMENTS_LABEL: RARE_ELEMENTS_LABEL_NEW})

    tb_supply["mineral"] = tb_supply["mineral"].astype("string").replace({RARE_ELEMENTS_LABEL: RARE_ELEMENTS_LABEL_NEW})

    # Supply data for each mineral is divided in "mining" and "refining".
    # But there are two special cases:
    # * For lithium, supply data is divided in "mining" and "chemicals". For consistency, we can rename them "mining" and "refining" and add a footnote.
    # * For graphite, supply data is divided in "mining (natural)" and "battery grade". For consistency, we can rename them "mining" and "refining" and add a footnote.
    tb_supply = tb_supply.astype({"process": "string", "mineral": "string"}).copy()
    tb_supply.loc[(tb_supply["mineral"] == "Lithium") & (tb_supply["process"] == "Chemicals"), "process"] = "Refining"
    tb_supply.loc[(tb_supply["mineral"] == "Graphite") & (tb_supply["process"] == "Battery grade"), "process"] = (
        "Refining"
    )
    tb_supply.loc[(tb_supply["mineral"] == "Graphite") & (tb_supply["process"] == "Mining (natural)"), "process"] = (
        "Mining"
    )

    # Rename a few things, for consistency with the minerals explorer.
    for table in [tb_demand, tb_supply]:
        table["process"] = table["process"].replace({"Mining": "Mine", "Refining": "Refinery"})

    return tb_demand, tb_supply


def improve_metadata_of_tables_by_technology_and_by_country(tb_demand_flat, tb_supply_flat):
    tb_demand_flat = tb_demand_flat.copy()
    tb_supply_flat = tb_supply_flat.copy()

    # Improve metadata.
    for table in [tb_demand_flat, tb_supply_flat]:
        for column in table.drop(columns=["country", "technology", "year"], errors="ignore").columns:
            metric, mineral, process, case, scenario = column.split("|")

            # Define some auxiliary items:
            # * Mineral-process.
            if process == "Mine":
                mineral_process = mineral.lower()
            elif process == "Refinery":
                mineral_process = f"refined {mineral.lower()}"
            else:
                paths.log.warning(f"Unexpected process for column {column}")
            # Fix special cases.
            mineral_process = mineral_process.replace("pgms", "PGMs")

            # * Official IEA's scenario name:
            if scenario == "Stated policies":
                scenario_name = "Stated Policies Scenario"
                scenario_dod = "iea-stated-policies-scenario"
            elif scenario == "Announced pledges":
                scenario_name = "Announced Pledges Scenario"
                scenario_dod = "iea-announced-pledges-scenario"
            elif scenario == "Net zero by 2050":
                scenario_name = "Net Zero Emissions by 2050 Scenario"
                scenario_dod = "iea-net-zero-emissions-by-2050-scenario"
            elif scenario == "All scenarios":
                # NOTE: The following should not be used in any string.
                scenario_name = ""
                scenario_dod = ""
            else:
                paths.log.warning(f"Unexpected scenario for column {column}")
            # * Case description:
            if case == "Base case":
                case_description = (
                    "Supply projections are based on operating and announced mining and refining projects."
                )
            elif case == "High material prices":
                case_description = "Projections assume high material prices."
            elif case == "Comeback of high Cd-Te technology":
                case_description = "Projections assume a comeback of high Cd-Te technology."
            elif case == "Constrained rare earth elements supply":
                case_description = "Projections assume a constrained rare earth elements supply."
            elif case == "Faster uptake of solid state batteries":
                case_description = "Projections assume a faster uptake of solid state batteries."
            elif case == "Limited battery size reduction":
                case_description = "Projections assume a limited battery size reduction."
            elif case == "Lower battery sizes":
                case_description = "Projections assume lower battery sizes."
            elif case == "Wider adoption of Ga-As technology":
                case_description = "Projections assume a wider adoption of Ga-As technology."
            elif case == "Wider adoption of perovskite solar cells":
                case_description = "Projections assume a wider adoption of perovskite solar cells."
            elif case == "Wider direct current (DC) technology development":
                case_description = "Projections assume a wider direct current (DC) technology development."
            elif case == "Wider use of silicon-rich anodes":
                case_description = "Projections assume a wider use of silicon-rich anodes."
            else:
                paths.log.warning(f"Unexpected case for column {column}")

            # Initialize a list of footnotes.
            footnotes = []

            # Create a variable title that is convenient to create the explorer later on.
            table[column].m.title = column

            # Create a public title.
            title_public = f"Projected {metric.split('_')[0]} of {mineral_process}"
            if "_share_of_" in metric:
                title_public += " " + " ".join(metric.split("_")[1:])
                table[column].m.unit = "%"
                table[column].m.short_unit = "%"
            else:
                table[column].m.unit = "tonnes"
                table[column].m.short_unit = "t"

            # Create a short description.
            short_descriptions = []
            # * Supply combinations.
            if metric == "supply":
                # NOTE: There is no need to repeat the title.
                # short_descriptions.append(f"Projected supply of {mineral_process}.")
                pass
            elif metric == "supply_as_a_share_of_global_supply":
                short_descriptions.append(
                    f"Projected supply of {mineral_process} as a share of the projected global supply of {mineral_process}."
                )
            elif metric == "supply_as_a_share_of_global_demand":
                short_descriptions.append(
                    f"Projected supply of {mineral_process}, as a share of the projected global demand of {mineral_process}, assuming IEA's [{scenario_name}](#dod:{scenario_dod}). A share below 100% means that the projected supply is not enough to meet the projected demand."
                )
            # * Demand combinations.
            elif metric == "demand":
                short_descriptions.append(
                    f"Projected demand of {mineral_process}, assuming IEA's [{scenario_name}](#dod:{scenario_dod})."
                )
            elif metric == "demand_as_a_share_of_global_supply":
                short_descriptions.append(
                    f"Projected demand of {mineral_process}, assuming IEA's [{scenario_name}](#dod:{scenario_dod}), as a share of the projected global supply of {mineral_process}. A share above 100% means that the projected supply is not enough to meet the projected demand."
                )
            elif metric == "demand_as_a_share_of_global_demand":
                short_descriptions.append(
                    f"Projected demand of {mineral_process}, assuming IEA's [{scenario_name}](#dod:{scenario_dod}), as a share of the projected global demand of {mineral_process}."
                )
            else:
                paths.log.warning(f"Unexpected metric for column {column}")
            # Append the case description at the end.
            short_descriptions.append(case_description)

            # Create a footnote in special cases.
            if (mineral == "Lithium") & (metric.startswith("demand")):
                footnotes.append("Lithium demand is in lithium content.")
                if process == "Refinery":
                    footnotes.append("Refined lithium refers to lithium chemicals.")

            if mineral == "Magnet rare earth elements":
                # TODO: Currently, this will never happen, as we renamed "Rare earths" to "Magnet rare earth elements". Consider renaming again to "Rare earths", for consistency with the minerals explorer.
                footnotes.append(
                    "Rare earth elements refer only to four magnet rare earths, namely neodymium, praseodymium, dysprosium and terbium."
                )

            if (mineral == "Graphite") & (process == "Refinery"):
                # TODO: Currently, this applies to supply only. We need to harmonize graphite data.
                footnotes.append("Refined graphite refers to battery-grade graphite.")

            if mineral not in ["Copper", "Cobalt", "Lithium", "Nickel", "Magnet rare earth elements", "Graphite"]:
                footnotes.append(
                    f"Values are limited to {mineral_process} used in clean technologies, excluding other possible applications."
                )

            # TODO: For Cobalt refinery, supply total is called "Total clean technologies" (unlike all other minerals,
            #  where it is "Total", including Cobalt mine). It's unclear if it's a typo.
            #  If not, we could exclude "Other uses" from the demand to calculate the share, and add a footnote.

            # Add a display name to the metadata.
            if not table[column].metadata.display:
                table[column].metadata.display = {}
            # table[column].metadata.display["name"] = mineral_process[0].upper() + mineral_process[1:]
            table[column].metadata.display["name"] = mineral

            # Add public title to the metadata.
            if not table[column].metadata.presentation:
                table[column].metadata.presentation = VariablePresentationMeta()
            table[column].metadata.presentation.title_public = title_public

            # Add short descriptions.
            table[column].metadata.description_short = " ".join(short_descriptions)

            # Add footnotes.
            if len(footnotes) > 0:
                combined_footnotes = " ".join(footnotes)
                if table[column].metadata.presentation.grapher_config is None:
                    table[column].metadata.presentation.grapher_config = {}
                table[column].metadata.presentation.grapher_config["note"] = combined_footnotes

    return tb_demand_flat, tb_supply_flat


def improve_metadata_of_tables_by_scenario(tb_demand_by_scenario_flat: Table) -> Table:
    tb_demand_by_scenario_flat = tb_demand_by_scenario_flat.copy()
    short_description = "Projections assume one of IEA's World Energy Outlook scenarios, namely [Stated Policies](#dod:iea-stated-policies-scenario), [Announced Pledges](#dod:iea-announced-pledges-scenario), or [Net Zero Emissions by 2050](#dod:iea-net-zero-emissions-by-2050-scenario). Click on 'Change scenario' to switch between them."
    for column in tb_demand_by_scenario_flat.drop(columns=["scenario", "year"], errors="raise").columns:
        _, mineral, technology = column.split("|")
        if technology == "Electric vehicles":
            technology_name = "electric vehicles"
        elif technology == "Solar PV":
            technology_name = "solar PV power generation"
        elif technology == "Wind":
            technology_name = "wind power generation"
        elif technology == "Grid battery storage":
            technology_name = "grid battery storage"
        elif technology == "Hydrogen technologies":
            technology_name = "hydrogen technologies"
        else:
            tb_demand_by_scenario_flat = tb_demand_by_scenario_flat.drop(columns=column, errors="raise")
            continue
        title_public = f"Mineral demand for {technology_name}"
        tb_demand_by_scenario_flat[column].metadata.title = column
        tb_demand_by_scenario_flat[column].metadata.description_short = short_description
        tb_demand_by_scenario_flat[column].metadata.unit = "tonnes"
        tb_demand_by_scenario_flat[column].metadata.short_unit = "t"
        tb_demand_by_scenario_flat[column].metadata.display = {"name": mineral}
        tb_demand_by_scenario_flat[column].metadata.presentation = VariablePresentationMeta(title_public=title_public)

    return tb_demand_by_scenario_flat


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("critical_minerals")

    #
    # Process data.
    #
    # Read and prepare supply table.
    tb_supply = prepare_supply_table(ds_meadow=ds_meadow)

    # Read and prepare demand table (by combining individual demand sheets).
    tb_demand = combine_individual_demand_tables(ds_meadow=ds_meadow)

    # Read and prepare data from the sheet on demand in clean technologies by mineral.
    tb_clean = prepare_clean_technologies_by_mineral_table(ds_meadow=ds_meadow)

    # Read and prepare table on total demand of minerals.
    tb_total = prepare_total_demand_table(ds_meadow=ds_meadow)

    # Add demand from other graphite uses that are accounted for in sheet 1 but not in sheets 4.x.
    tb_demand = add_demand_from_other_graphite(tb_demand=tb_demand, tb_total=tb_total)

    # Add demand from other low emissions power generation (not accounted for in the individual technology demand sheets) to the demand table.
    tb_demand = add_demand_from_other_clean(tb_demand=tb_demand, tb_clean=tb_clean)

    # Add other uses of minerals (outside of clean tech) from sheet 1 to the total demand.
    tb_demand = add_demand_from_other_uses(tb_demand=tb_demand, tb_total=tb_total)

    # Further processing of the demand table.
    tb_demand = clean_demand_table(tb_demand=tb_demand)

    # Harmonize units.
    tb_demand, tb_supply, tb_total = harmonize_units(tb_demand=tb_demand, tb_supply=tb_supply, tb_total=tb_total)

    # For consistency (within the dataset and also with the minerals explorer) rename certain minerals.
    tb_demand, tb_supply = harmonize_minerals_and_processes(tb_demand=tb_demand, tb_supply=tb_supply)

    # Add "share" columns.
    tb_demand, tb_supply = add_share_columns(tb_demand=tb_demand, tb_supply=tb_supply)

    # Sanity checks.
    run_sanity_checks_on_outputs(tb_demand=tb_demand, tb_total=tb_total)

    # Create a wide-format table of demand by technology.
    tb_demand_by_technology_flat = create_demand_by_technology_flat(tb_demand=tb_demand)

    # Create a wide-format table of supply by country.
    tb_supply_by_country_flat = create_supply_by_country_flat(tb_supply=tb_supply)

    # Create a wide-format table of demand by scenario.
    tb_demand_by_scenario_flat = create_demand_by_scenario_flat(tb_demand=tb_demand)

    # Improve metadata of flat tables.
    tb_demand_by_technology_flat, tb_supply_by_country_flat = improve_metadata_of_tables_by_technology_and_by_country(
        tb_demand_flat=tb_demand_by_technology_flat, tb_supply_flat=tb_supply_by_country_flat
    )
    tb_demand_by_scenario_flat = improve_metadata_of_tables_by_scenario(
        tb_demand_by_scenario_flat=tb_demand_by_scenario_flat
    )

    # Improve tables format.
    # NOTE: This should be done after improving metadata, otherwise titles will get snake-cased.
    tb_supply = tb_supply.format(["case", "country", "year", "mineral", "process", "scenario"])
    tb_demand = tb_demand.format(["case", "year", "mineral", "process", "scenario", "technology"])
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.format(
        keys=["technology", "year"], short_name="demand_by_technology"
    )
    tb_supply_by_country_flat = tb_supply_by_country_flat.format(short_name="supply_by_country")
    tb_demand_by_scenario_flat = tb_demand_by_scenario_flat.format(
        keys=["scenario", "year"], short_name="demand_by_scenario"
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[
            tb_demand,
            tb_supply,
            tb_demand_by_technology_flat,
            tb_supply_by_country_flat,
            tb_demand_by_scenario_flat,
        ],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
