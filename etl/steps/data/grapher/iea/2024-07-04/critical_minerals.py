"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("critical_minerals")
    tb_demand = ds_garden.read_table("demand_for_key_minerals")
    tb_supply = ds_garden.read_table("supply_for_key_minerals")

    #
    # Process data.
    #
    # Remove unnecessary country column.
    tb_demand = tb_demand.drop(columns=["country"], errors="raise")
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

    # Supply data for each mineral is divided in "mining" and "refining".
    # But there are two special cases:
    # * For lithium, supply data is divided in "mining" and "chemicals". For consistency, we can rename them "mining" and "refining" and add a footnote.
    # * For graphite, supply data is divided in "mining (natural)" and "battery grade". For consistency, we can rename them "mining" and "refining" and add a footnote.
    # TODO: Add those footnotes.
    tb_supply = tb_supply.astype({"process": "string"}).copy()
    tb_supply.loc[(tb_supply["mineral"] == "Lithium") & (tb_supply["process"] == "Chemicals"), "process"] = "Refining"
    tb_supply.loc[
        (tb_supply["mineral"] == "Graphite") & (tb_supply["process"] == "Battery grade"), "process"
    ] = "Refining"

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

    # TODO: Move some of the following processing to the garden step.
    # TODO: We will create two tables:
    #  * Demand by technology. Explorer will have mineral, case, scenario, and metric.
    #  * Supply by country.
    #
    #  We will then create a "Mineral supply and demand prospects" explorer, with the following elements:
    #  * A radio button for "Demand by tech" (demand table) and "Supply by country" (supply table).
    #  * A "Mineral" dropdown.
    #  * A "Case" dropdown (which will be only "Base case" for the supply table).
    #  * A "Scenario" dropdown (which will be only "All scenarios" (or empty?) for the supply table).
    #  * A "Metric" dropdown, with options "Total", "Share of global demand", and "Share of global supply".

    # Prepare supply by country table.
    def create_supply_by_country_table(tb_supply: Table, tb_demand_global: Table, tb_supply_global: Table) -> Table:
        tb_supply = tb_supply.copy()

        # Add global supply.
        tb_supply = tb_supply.merge(tb_supply_global, on=["case", "year", "mineral", "process"], how="left")
        assert set(tb_supply[tb_supply["supply"] == tb_supply["global_supply"]]["country"]) == set(["World"])

        # We assume that all demand is of refined minerals.
        tb_supply = tb_supply.merge(tb_demand_global, on=["case", "year", "mineral", "process"], how="left")

        # Add "share" columns.
        tb_supply["supply_as_share_of_global_demand"] = 100 * tb_supply["supply"] / tb_supply["global_demand"]
        tb_supply["supply_as_share_of_global_supply"] = 100 * tb_supply["supply"] / tb_supply["global_supply"]

        return tb_supply

    def create_demand_by_technology_table(tb_demand: Table, tb_demand_global: Table, tb_supply_global: Table) -> Table:
        tb_demand = tb_demand.copy()

        # From the original supply table, we need the global supply of each mineral-year (given only for the "Base case").
        # TODO: For consistency with the current minerals explorer, we should rename them "Mine" and "Refinery".

        # Add total demand to the demand table.
        tb_demand = tb_demand.merge(tb_demand_global, on=["case", "scenario", "mineral", "process", "year"], how="left")
        # TODO: For "Graphite (all grades: natural and synthetic)", "Other uses" has all the demand. There may be a bug.
        #  t[t["demand"]==t["global_demand"]]
        # TODO: After fixing that, consider renaming to "Graphite" and add a subtitle or footnote.

        # Add total supply to the demand table.
        tb_demand = tb_demand.merge(tb_supply_global, on=["case", "year", "mineral", "process"], how="left")

        # Add "share" columns.
        tb_demand["demand_as_share_of_global_demand"] = 100 * tb_demand["demand"] / tb_demand["global_demand"]
        tb_demand["demand_as_share_of_global_supply"] = 100 * tb_demand["demand"] / tb_demand["global_supply"]

        return tb_demand

    # Prepare "Demand by technology" table.
    tb_demand_by_technology = create_demand_by_technology_table(
        tb_demand=tb_demand, tb_demand_global=tb_demand_global, tb_supply_global=tb_supply_global
    )

    # Prepare "Supply by country" table.
    tb_supply_by_country = create_supply_by_country_table(
        tb_supply=tb_supply, tb_demand_global=tb_demand_global, tb_supply_global=tb_supply_global
    )

    def create_demand_by_technology_flat(tb_demand_by_technology: Table) -> Table:
        # Create a wide-format table.
        tb_demand_by_technology_flat = tb_demand_by_technology.pivot(
            index=["technology", "year"],
            columns=["mineral", "process", "case", "scenario"],
            values=["demand", "demand_as_share_of_global_demand", "demand_as_share_of_global_supply"],
            join_column_levels_with="|",
        )
        # Adapt table to grapher.
        tb_demand_by_technology_flat = tb_demand_by_technology_flat.rename(columns={"technology": "country"})

        # Remove empty columns.
        tb_demand_by_technology_flat = tb_demand_by_technology_flat.dropna(axis=1, how="all")

        return tb_demand_by_technology_flat

    # Create a wide-format table of demand by technology.
    tb_demand_by_technology_flat = create_demand_by_technology_flat(tb_demand_by_technology=tb_demand_by_technology)

    def create_supply_by_country_flat(tb_supply_by_country: Table) -> Table:
        # Create a wide-format table.
        tb_supply_by_country_flat = tb_supply_by_country.pivot(
            index=["country", "year"],
            columns=["mineral", "process", "case", "scenario"],
            values=["supply", "supply_as_share_of_global_demand", "supply_as_share_of_global_supply"],
            join_column_levels_with="|",
        )

        # Remove empty columns.
        tb_supply_by_country_flat = tb_supply_by_country_flat.dropna(axis=1, how="all")

        return tb_supply_by_country_flat

    # Create a wide-format table of supply by country.
    tb_supply_by_country_flat = create_supply_by_country_flat(tb_supply_by_country=tb_supply_by_country)

    # Improve metadata.
    # TODO: Do this properly.
    for table in [tb_demand_by_technology_flat, tb_supply_by_country_flat]:
        for column in table.columns:
            table[column].m.title = column
            if "_share_of_" in column:
                table[column].m.unit = "%"
                table[column].m.short_unit = "%"
            else:
                table[column].m.unit = "tonnes"
                table[column].m.short_unit = "t"

    # Improve its format.
    # NOTE: This should be done after improving metadata, otherwise titles will get snake-cased.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.format(short_name="demand_by_technology")
    tb_supply_by_country_flat = tb_supply_by_country_flat.format(short_name="supply_by_country")

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_demand_by_technology_flat, tb_supply_by_country_flat], check_variables_metadata=True
    )
    ds_grapher.save()
