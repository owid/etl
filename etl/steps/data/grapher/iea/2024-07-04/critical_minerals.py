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

    def create_demand_by_technology_table(tb_demand: Table, tb_supply: Table) -> Table:
        tb_demand = tb_demand.copy()
        tb_supply = tb_supply.copy()

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

        # For each case-scenario-mineral-year, we need the global demand of all technologies.
        # NOTE: Global mineral demand, including uses outside clean tech, is only given for "Base case" for a few minerals (for which "Other uses" is explicitly given).
        minerals_with_total_demand = tb_demand[tb_demand["technology"] == "Other uses"]["mineral"].unique().tolist()
        tb_demand_global = (
            tb_demand[(tb_demand["case"] == "Base case") & (tb_demand["mineral"].isin(minerals_with_total_demand))]
            .groupby(["case", "scenario", "mineral", "year"], observed=True, as_index=False)
            .agg({"demand": "sum"})
            .rename(columns={"demand": "global_demand"})
        )

        # From the original supply table, we need the global supply of each mineral-year (given only for the "Base case").
        # Supply data for each mineral is divided in "mining" and "refining".
        # TODO: For consistency with the current minerals explorer, we should rename them "Mine" and "Refinery".
        # When combining with demand data, we can simply take "refining" values, as they are the ones actually demanded to produce the technology.
        # But there are two special cases:
        # * For lithium, supply data is divided in "mining" and "chemicals". For consistency, we can rename them "mining" and "refining" and add a footnote.
        # * For graphite, supply data is divided in "mining (natural)" and "battery grade". For consistency, we can rename them "mining" and "refining" and add a footnote.
        # TODO: Add those footnotes.
        tb_supply_global = tb_supply.astype({"process": "string"}).copy()
        tb_supply_global.loc[
            (tb_supply["mineral"] == "Lithium") & (tb_supply["process"] == "Chemicals"), "process"
        ] = "Refining"
        tb_supply_global.loc[
            (tb_supply["mineral"] == "Graphite") & (tb_supply["process"] == "Battery grade"), "process"
        ] = "Refining"
        tb_supply_global = tb_supply_global[tb_supply_global["process"] == "Refining"].reset_index(drop=True)
        tb_supply_global = tb_supply_global[tb_supply_global["country"] == "World"][
            ["case", "year", "mineral", "supply"]
        ].rename(columns={"supply": "global_supply"})

        # Add total demand to the demand table.
        # NOTE:
        tb_demand = tb_demand.merge(tb_demand_global, on=["case", "scenario", "mineral", "year"], how="left")
        # TODO: For "Graphite (all grades: natural and synthetic)", "Other uses" has all the demand. There may be a bug.
        #  t[t["demand"]==t["global_demand"]]
        # TODO: After fixing that, consider renaming to "Graphite" and add a subtitle or footnote.

        # Add total supply to the demand table.
        tb_demand = tb_demand.merge(tb_supply_global, on=["case", "year", "mineral"], how="left")

        # Add "share" columns.
        tb_demand["demand_as_share_of_global_demand"] = 100 * tb_demand["demand"] / tb_demand["global_demand"]
        tb_demand["demand_as_share_of_global_supply"] = 100 * tb_demand["demand"] / tb_demand["global_supply"]

        return tb_demand

    # Prepare "Demand by technology" table.
    tb_demand_by_technology = create_demand_by_technology_table(tb_demand=tb_demand, tb_supply=tb_supply)

    # Create a wide-format table.
    tb_demand_by_technology_flat = tb_demand_by_technology.pivot(
        index=["technology", "year"],
        columns=["mineral", "case", "scenario"],
        values=["demand", "demand_as_share_of_global_demand", "demand_as_share_of_global_supply"],
        join_column_levels_with="|",
    )
    # Adapt table to grapher.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.rename(columns={"technology": "country"})

    # Improve metadata.
    # TODO: Do this properly.
    for column in tb_demand_by_technology_flat.drop(columns=["country", "year"]).columns:
        tb_demand_by_technology_flat[column].m.title = column
        if "_share_of_" in column:
            tb_demand_by_technology_flat[column].m.unit = "%"
            tb_demand_by_technology_flat[column].m.short_unit = "%"
        else:
            tb_demand_by_technology_flat[column].m.unit = "tonnes"
            tb_demand_by_technology_flat[column].m.short_unit = "t"

    # Remove empty columns.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.dropna(axis=1, how="all")

    # Improve its format.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.format(short_name="demand_by_technology")

    def create_mineral_demand_by_technology(tb_supply, tb_demand):
        _tb_supply = tb_supply[
            (tb_supply["case"] == "Base case")
            & (tb_supply["country"] == "World")
            & (tb_supply["mineral"] == "Copper")
            & (tb_supply["process"] == "Refining")
        ].reset_index(drop=True)[["year", "supply"]]
        _tb_demand = tb_demand[
            (tb_demand["case"] == "Base case")
            & (tb_demand["country"] == "World")
            & (tb_demand["mineral"] == "Copper")
            & (tb_demand["scenario"].isin(["Current", "Net zero by 2050"]))
        ][["technology", "year", "demand"]]
        tb_combined = _tb_demand.merge(_tb_supply, on=["year"], how="inner")
        tb_combined["demand_as_share_of_supply"] = 100 * tb_combined["demand"] / tb_combined["supply"]
        tb_combined = tb_combined[["technology", "year", "demand", "demand_as_share_of_supply"]].rename(
            columns={"technology": "country"}
        )
        tb_combined["demand"].m.title = "Demand of copper by technology"
        tb_combined["demand"].m.unit = "tonnes"
        tb_combined["demand"].m.short_unit = "t"
        tb_combined["demand_as_share_of_supply"].m.title = "Demand of copper by technology as share of global supply"
        tb_combined["demand_as_share_of_supply"].m.unit = "%"
        tb_combined["demand_as_share_of_supply"].m.short_unit = "%"
        tb_combined = tb_combined.format(sort_rows=False)

        return tb_combined

    # tb_demand_of_copper_by_technology = create_mineral_demand_by_technology(tb_supply, tb_demand)

    def create_mineral_supply_by_country(tb_supply, tb_demand):
        tb_supply_global = tb_supply[
            (tb_supply["case"] == "Base case")
            & (tb_supply["country"] == "World")
            & (tb_supply["mineral"] == "Copper")
            & (tb_supply["process"] == "Refining")
        ].reset_index(drop=True)[["country", "year", "supply"]]
        tb_supply_by_country = tb_supply[
            (tb_supply["case"] == "Base case")
            & (tb_supply["country"] != "World")
            & (tb_supply["mineral"] == "Copper")
            & (tb_supply["process"] == "Refining")
        ].reset_index(drop=True)[["country", "year", "supply"]]
        tb_supply_share = tb_supply_by_country.merge(
            tb_supply_global, on=["year"], how="inner", suffixes=("", "_global")
        )

        tb_demand_global = tb_demand[
            (tb_demand["case"] == "Base case")
            & (tb_demand["country"] == "World")
            & (tb_demand["mineral"] == "Copper")
            & (tb_demand["scenario"].isin(["Current", "Net zero by 2050"]))
        ][["technology", "year", "demand"]]
        tb_demand_global_total = (
            tb_demand_global.groupby("year", observed=True, as_index=False)
            .agg({"demand": "sum"})
            .rename(columns={"demand": "demand_global"})
        )

        tb_supply_share = tb_supply_share.merge(tb_demand_global_total, on=["year"], how="inner")
        tb_supply_share["supply_as_share_of_global_demand"] = (
            100 * tb_supply_share["supply"] / tb_supply_share["demand_global"]
        )
        tb_supply_share["supply_as_share_of_global_supply"] = (
            100 * tb_supply_share["supply"] / tb_supply_share["supply_global"]
        )

        tb_supply_share[
            "supply_as_share_of_global_demand"
        ].m.title = "Supply of refined copper as a share of global demand"
        tb_supply_share["supply_as_share_of_global_demand"].m.unit = "%"
        tb_supply_share["supply_as_share_of_global_demand"].m.short_unit = "%"
        tb_supply_share[
            "supply_as_share_of_global_supply"
        ].m.title = "Supply of refined copper as a share of global supply"
        tb_supply_share["supply_as_share_of_global_supply"].m.unit = "%"
        tb_supply_share["supply_as_share_of_global_supply"].m.short_unit = "%"

        tb_supply_share = tb_supply_share[
            ["country", "year", "supply_as_share_of_global_supply", "supply_as_share_of_global_demand"]
        ].format()

        return tb_supply_share

    # tb_supply_share = create_mineral_supply_by_country(tb_supply, tb_demand)

    def create_technology_demand_by_mineral(tb_demand):
        # TODO: Another explorer (view) could have a checkbox for technology. It shows a stacked are chart of minerals demanded as a share of supply, for the technologies selected.
        # NOTE: Here "global" should probably be "total", since it refers to the sum of all technologies (and all demand data is global), but for simplicity call it "global".
        tb_demand_pv = tb_demand[
            (tb_demand["case"] == "Base case")
            & (tb_demand["scenario"].isin(["Current", "Net zero by 2050"]))
            & (tb_demand["technology"] == "Solar PV")
        ][["mineral", "year", "demand"]].rename(columns={"mineral": "country"})

        # For now, select specifically solar pv.
        tb_demand_pv["demand"].m.title = "Mineral demand for solar PV"
        tb_demand_pv["demand"].m.unit = "tonnes"
        tb_demand_pv["demand"].m.short_unit = "t"

        tb_demand_pv = tb_demand_pv.format(short_name="demand_of_minerals_for_solar_pv")

        return tb_demand_pv

    # NOTE: Global total demand is sometimes zero. This introduces nans in the shares.
    # tb_demand_pv = create_technology_demand_by_mineral(tb_demand)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_demand_by_technology_flat], check_variables_metadata=True)
    ds_grapher.save()
