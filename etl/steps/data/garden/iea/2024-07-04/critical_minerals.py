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
    - "Graphite (all grades: natural and synthetic)". This is the most problematic case. It shows, for example, 1147kt for EV, whereas, in the EV sheet, we see "Battery-grade graphite" 685.56kt (and no other use of graphite appears). A similar inconsistency happens with grid battery storage. So, it seems that there are additional uses of graphite that are not accounted for in the EV and battery storage sheets. We can include a "Graphite, other" mineral, with the difference.

TODO: Therefore, the strategy should be:
* Combine the 4.x sheets into one demand table.
* Assign a "case" column to sheet 3.2 (always assume Base case).
* In demand table, create an aggregate "Magnet rare earth elements" that combines Praseodymium, Neodymium, Terbium and Dysprosium.
* From sheet 3.2, remove rows for Praseodymium, Neodymium, Terbium and Dysprosium, and rename "Total rare earth elements" -> "Magnet rare earth elements". Also, fix the misspelled "iridum".
* Calculate "Other low emissions power generation", which should be the total from 3.2 minus the total from 4.x for each mineral. Add those other low emission uses to the demand table.
* Assign a "case" column to sheet 1.
* Add the other uses of graphite from sheet 1 to the demand table (EV and battery storage).
* Add "Other uses" from sheet 1 to the demand table.
* Sanity check that the totals from the resulting demand table coincides with the totals from sheet 1.
Then, in the metadata:
* Add the notes extracted from the total demand sheet:
  - Lithium demand is in lithium (Li) content, not carbonate equivalent (LCE).
  - Demand for magnet rare earth elements covers praseodymium (Pr), neodymium (Nd), terbium (Tb) and dysprosium (Dy).
  - Graphite demand  includes all grades of mined and synthetic graphite.
* Add the notes extracted from the supply sheet:
  - Supply projections for the key energy transition minerals are built using the data for the pipeline of operating and announced mining and refining projects by country.
* Add general note:
# - "Base case" is assessed through their probability of coming online based on various factors such as the status of financing, permitting and feasibility studies.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
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

# Name of rare earth elements aggregate.
RARE_ELEMENTS_LABEL = "Magnet rare earth elements"

# List of rare element magnets.
RARE_ELEMENTS_LIST = ["Praseodymium", "Neodymium", "Terbium", "Dysprosium"]

# Name of all graphite uses, listed in sheet 1 (of total demand including uses outside of clean tech).
GRAPHITE_ALL_LABEL = "Graphite (all grades: natural and synthetic)"

# Name of a new mineral category, which encompasses the difference in demand between sheet 1 and the combined 4.x sheets.
OTHER_GRAPHITE_LABEL = "Graphite, other"

# Name of other uses in clean tech.
OTHER_LOW_LABEL = "Other low emissions power generation"


def prepare_supply_table(ds_meadow: Dataset) -> Table:
    # Read supply table.
    tb_supply = ds_meadow["supply_for_key_minerals"].reset_index()

    # Ignore aggregates in the data.
    tb_supply = tb_supply[~tb_supply["country"].str.startswith(("Total", "Top"))].reset_index(drop=True)

    # Harmonize country names.
    tb_supply = geo.harmonize_countries(df=tb_supply, countries_file=paths.country_mapping_path)

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
        "PGMs (other than iridum)": "PGMs other than iridium",
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
        combined[combined["difference"] >= combined["demand"] * 1.01]
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
    graphite_uses = ["Battery-grade graphite"]

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
    tb_graphite_total = tb_total[tb_total["mineral"] == GRAPHITE_ALL_LABEL].reset_index(drop=True)

    # Calculate the total graphite demand in the 4.x sheets.
    tb_graphite_demand_total = (
        tb_demand[tb_demand["mineral"].isin(graphite_uses)]
        .groupby(["case", "scenario", "mineral", "year"], observed=True, as_index=False)
        .agg({"demand": "sum"})
    )
    combined = tb_graphite_demand_total.merge(
        tb_graphite_total, how="inner", on=["case", "scenario", "mineral", "year"], suffixes=("", "_summary")
    )
    # Check that the totals in sheet 1 are always larger than the totals calculated from combining the 4.x sheets (or within 1%).
    error = "Sheet on clean technology graphite demand by mineral should be >= the sum of the graphite demands from the 4.x sheets."
    assert combined[(combined["demand_summary"] < (combined["demand"] * 0.99))].empty, error
    combined["difference"] = combined["demand_summary"] - combined["demand"]

    # Create a new technology that contains all the remaining graphite demand not included in the 4.x sheets.
    # NOTE: Only include it if the difference is larger than 1%.
    tb_graphite_other = (
        combined[combined["difference"] >= combined["demand"] * 1.01]
        .assign(**{"technology": OTHER_GRAPHITE_LABEL})
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

    # Even if not explicitly mentioned, it seems that "Base case" is assumed in this sheet.
    tb_total["case"] = "Base case"

    return tb_total


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

    # TODO:
    # * Add "Other uses" from sheet 1 to the demand table.
    # * Sanity check that the totals from the resulting demand table coincides with the totals from sheet 1.

    # Format output tables conveniently.
    tb_supply = tb_supply.format(["case", "country", "year", "mineral", "process"])
    tb_demand = tb_demand.format(["case", "scenario", "technology", "mineral", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_demand, tb_supply], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
