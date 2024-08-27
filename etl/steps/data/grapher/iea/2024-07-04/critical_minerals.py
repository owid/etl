"""Load a garden dataset and create a grapher dataset."""

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
    def create_demand_of_copper_as_share_of_supply(tb_supply, tb_demand):
        tb_supply = tb_supply[
            (tb_supply["case"] == "Base case")
            & (tb_supply["country"] == "World")
            & (tb_supply["mineral"] == "Copper")
            & (tb_supply["process"] == "Refining")
        ].reset_index(drop=True)[["year", "supply"]]
        tb_demand = tb_demand[
            (tb_demand["case"] == "Base case")
            & (tb_demand["country"] == "World")
            & (tb_demand["mineral"] == "Copper")
            & (tb_demand["scenario"].isin(["Current", "Net zero by 2050"]))
        ][["technology", "year", "demand"]]
        tb_combined = tb_demand.merge(tb_supply, on=["year"], how="inner")
        tb_combined["demand_as_share_of_supply"] = 100 * tb_combined["demand"] / tb_combined["supply"]
        tb_combined = tb_combined[["technology", "year", "demand_as_share_of_supply"]].rename(
            columns={"technology": "country"}
        )
        tb_combined["demand_as_share_of_supply"].m.title = "Demand of copper as share of supply"
        tb_combined["demand_as_share_of_supply"].m.unit = "%"
        tb_combined["demand_as_share_of_supply"].m.short_unit = "%"
        tb_combined = tb_combined.sort_values(["year", "demand_as_share_of_supply"]).reset_index(drop=True)
        tb_combined = tb_combined.format(sort_rows=False)

        return tb_combined

    tb_demand_of_copper_as_share_of_supply = create_demand_of_copper_as_share_of_supply(tb_supply, tb_demand)

    def create_supply_of_copper_by_country(tb_supply, tb_demand):
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

    tb_supply_share = create_supply_of_copper_by_country(tb_supply, tb_demand)

    # TODO: Supply data for each mineral is divided in "mining" and "refining". When combining with demand data, we can simply take "refining" values, as they are the ones actually demanded to produce the technology.
    #  In the case of lithium, supply data is divided in "mining" and "chemicals". For consistency, we can rename "chemicals" -> "refining" and add a footnote.
    #  In the case of graphite, supply data is divided in "mining (natural)" and "battery grade". For consistency, we can rename "battery grade" -> "refining" and add a footnote.
    # tb_supply = tb_supply.astype({"process": "string"})
    # tb_supply.loc[
    #     (tb_supply["mineral"] == "Lithium") & (tb_supply["process"] == "Chemicals"), "process"
    # ] = "Refining"
    # tb_supply.loc[
    #     (tb_supply["mineral"] == "Graphite") & (tb_supply["process"] == "Battery grade"), "process"
    # ] = "Refining"
    # tb_supply_refined = tb_supply[tb_supply["process"] == "Refining"].reset_index(drop=True)
    # tb_supply_refined_global = tb_supply_refined[tb_supply_refined["country"] == "World"][
    #     ["case", "year", "mineral", "supply"]
    # ].rename(columns={"supply": "supply_global"})

    def create_demand_by_technology(tb_demand):
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
    tb_demand_pv = create_demand_by_technology(tb_demand)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_demand_of_copper_as_share_of_supply, tb_supply_share, tb_demand_pv],
        check_variables_metadata=True,
    )
    ds_grapher.save()
