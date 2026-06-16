"""Garden step for the FAOSTAT food-trade Sankey viz.

Builds the slice of bilateral trade flows from the trade matrix, with two columns from SCL Production:

  exporter (str)               — exporting country (OWID-harmonized)
  importer (str)               — importing country (OWID-harmonized)
  item     (str)               — viz-display item (see food_trade.items.yaml)
  value    (float, tonnes)     — bilateral A→B trade flow in tonnes
  exporter_production (float)  — exporter's SCL Production tonnes of that item
                                 (NaN if SCL has no Production row for it)
  importer_supply (float)      — importer's domestic supply in tonnes,
                                 computed from SCL via the FAOSTAT Food
                                 Balance Sheet identity:
                                     Production + Imports - Exports - Stock Variation
                                 All four components come from SCL. Stock
                                 Variation in SCL is `Closing - Opening`
                                 (positive when stocks accumulated during
                                 the year, negative when drawn down) — so
                                 it is *subtracted* from the supply, as
                                 accumulated stocks don't reach domestic
                                 use.

The display items shown in the dropdown are curated in
`food_trade.items.yaml`. Each entry names a single FAO commodity item code
(the same codebook used by both TM and SCL), so the rollup is a direct
integer-code filter against `item_code`.

For each (exporter, importer, item) the trade matrix typically has two
reports — one from each side — that can disagree. We default to the
importer-reported value; FAOSTAT itself notes that "imports are
typically documented more thoroughly and verified more rigorously than
exports" (FAO 2025, Food Balance Sheets and Supply Utilization Accounts
Resource Handbook, §6.1, citing UNSD 2013). When only one side reports,
we use that side's number.

Items in TM that aren't covered by `food_trade.items.yaml` are dropped;
items in `food_trade.items.yaml` that aren't in the TM snapshot raise a
clear assertion (sanity check below).
"""

import pandas as pd
import yaml
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def sanity_check_items_config(config: dict, tm_items: dict[int, str]) -> None:
    """Validate the items config structurally and against the TM snapshot.

    `tm_items` maps each TM item code to its FAO item name in the current snapshot.

    Checks:
      1. Required top-level shape: a single 'items' list.
      2. Each items entry has required fields (display, item_code, fao_item).
      3. display names are unique.
      4. Each item_code exists in the TM snapshot (catches typos / removed codes).
      5. Each code's FAO item name still matches the expected `fao_item` (catches FAO
         silently reassigning or renaming a code to a different commodity).
    """
    assert isinstance(config, dict) and "items" in config, "items config must be a mapping with an 'items' key"
    items = config["items"]
    assert isinstance(items, list) and items, "config['items'] must be a non-empty list"

    required = {"display", "item_code", "fao_item"}
    for entry in items:
        missing = required - entry.keys()
        assert not missing, f"items entry missing keys {sorted(missing)}: {entry}"

    displays = [e["display"] for e in items]
    dupes = sorted({d for d in displays if displays.count(d) > 1})
    assert not dupes, f"Duplicate display names in items config: {dupes}"

    missing_codes = sorted(int(e["item_code"]) for e in items if int(e["item_code"]) not in tm_items)
    assert not missing_codes, f"{len(missing_codes)} item_code(s) not found in TM snapshot: {missing_codes[:10]}"

    renamed = [
        f"{e['item_code']}: expected {e['fao_item']!r}, TM has {tm_items[int(e['item_code'])]!r}"
        for e in items
        if tm_items[int(e["item_code"])] != e["fao_item"]
    ]
    assert not renamed, (
        "FAO item name no longer matches `fao_item` for: "
        + "; ".join(renamed)
        + ". Verify each code still refers to the intended commodity before updating fao_item."
    )


def build_food_trade_table(tb_tm: Table, tb_scl: Table) -> Table:
    """Reshape the trade matrix into the viz-ready slice with Production and
    apparent domestic supply context."""
    # Pick the latest well-covered year: the latest one whose distinct-reporter count is at
    # least 90% of the series maximum, i.e. not the partially reported tail year. We count
    # reporters rather than rows so a genuine trade contraction can't look like low coverage.
    reporters_per_year = tb_tm.groupby("year", observed=True)["reporter_country"].nunique()
    year = int(reporters_per_year[reporters_per_year >= 0.9 * reporters_per_year.max()].index.max())

    # Load list of curated items.
    with open(paths.side_file("food_trade.items.yaml")) as f:
        items_config = yaml.safe_load(f)
    code_to_display = {int(e["item_code"]): e["display"] for e in items_config["items"]}

    # 1) Filter TM to the curated items, as quantities in tonnes for the chosen year, then drop self-trade rows.
    trade_flows = tb_tm[
        (tb_tm["year"] == year)
        & tb_tm["element"].isin(["Export quantity", "Import quantity"])
        & (tb_tm["unit"] == "t")
        & tb_tm["item_code"].isin(code_to_display)
    ].copy()
    trade_flows["reporter_country"] = trade_flows["reporter_country"].astype(str)
    trade_flows["partner_country"] = trade_flows["partner_country"].astype(str)
    trade_flows = trade_flows[trade_flows["reporter_country"] != trade_flows["partner_country"]]

    # Validate the curated config against the FAO item names in the snapshot (item_code -> name).
    tm_items = dict(zip(trade_flows["item_code"], trade_flows["item"].astype(str)))
    sanity_check_items_config(items_config, tm_items=tm_items)

    trade_flows["item"] = trade_flows["item_code"].map(code_to_display)

    # 2) Build per-(country, item) Production and apparent domestic supply from SCL.
    #    Supply follows the FBS identity documented in the module docstring; all four
    #    components are SCL quantities in tonnes, restricted to the curated items.
    components = ["Production", "Import quantity", "Export quantity", "Stock Variation"]
    scl = tb_scl[
        (tb_scl["year"] == year) & (tb_scl["unit_short_name"] == "t") & tb_scl["element"].isin(components)
    ].copy()
    # SCL stores item_code as zero-padded strings ("00000015"); convert to int to join with TM.
    scl["item_code"] = scl["item_code"].astype(str).astype(int)
    scl["country"] = scl["country"].astype(str)
    scl["element"] = scl["element"].astype(str)
    scl = scl[scl["item_code"].isin(code_to_display)]

    # Pivot the four components into one column each, keyed on (country, item_code). We pivot
    # rather than groupby-sum so that a duplicate (country, item_code, element) row raises
    # instead of being silently summed: SCL reports one value per key, so duplicates would be
    # a data error, not something to aggregate. join_column_levels_with moves (country,
    # item_code) back to columns and restores each component column's value metadata.
    supply_context = scl.pivot(
        index=["country", "item_code"], columns="element", values="value", join_column_levels_with=""
    )
    # Guard against SCL missing a whole component for the year (the pivot would omit its column).
    for component in components:
        if component not in supply_context.columns:
            supply_context[component] = pd.NA
            supply_context[component] = supply_context[component].copy_metadata(scl["value"])
    supply_context["supply"] = (
        supply_context["Production"].fillna(0)
        + supply_context["Import quantity"].fillna(0)
        - supply_context["Export quantity"].fillna(0)
        - supply_context["Stock Variation"].fillna(0)
    )
    # Negative supply signals the FBS components don't reconcile (re-exports not captured by
    # stock variation, timing mismatches, primary-equivalent aggregation). NaN it out rather
    # than clipping to 0, so the downstream import-share ratio is undefined, not misleadingly zero.
    supply_context.loc[supply_context["supply"] < 0, "supply"] = pd.NA
    supply_context["item"] = supply_context["item_code"].map(code_to_display)

    # Exporter context column: only emit `exporter_production` for countries with
    # an SCL Production figure (we don't want to imply "0 production" for absent rows).
    exporter_production = (
        supply_context[["country", "item", "Production"]]
        .dropna(subset=["Production"])
        .rename(columns={"country": "exporter", "Production": "exporter_production"})
    )

    # Importer context column: use the FBS-identity supply for every (country, item)
    # row SCL knows about. Pairs entirely absent from SCL will fall out as NaN in
    # the downstream merge.
    importer_supply = supply_context[["country", "item", "supply"]].rename(
        columns={"country": "importer", "supply": "importer_supply"}
    )

    # 3) Join the directional reports into one row per (exporter, importer, item).
    export_reports = trade_flows.loc[
        trade_flows["element"] == "Export quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(columns={"reporter_country": "exporter", "partner_country": "importer", "value": "value_exporter"})
    import_reports = trade_flows.loc[
        trade_flows["element"] == "Import quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(columns={"reporter_country": "importer", "partner_country": "exporter", "value": "value_importer"})

    bilateral = pr.merge(export_reports, import_reports, on=["exporter", "importer", "item"], how="outer")
    # Default to the importer-reported value; fall back to the exporter-reported value
    # only when the importer doesn't report. See docstring for the rationale.
    bilateral["value"] = bilateral["value_importer"].fillna(bilateral["value_exporter"])
    bilateral = bilateral.dropna(subset=["value"])
    bilateral = bilateral[bilateral["value"] > 0]

    food_trade = bilateral[["exporter", "importer", "item", "value"]]
    food_trade = pr.merge(food_trade, exporter_production, on=["exporter", "item"], how="left")
    food_trade = pr.merge(food_trade, importer_supply, on=["importer", "item"], how="left")
    food_trade = food_trade.sort_values(["exporter", "importer", "item"]).reset_index(drop=True)
    # Carry the year so the data is self-describing and downstream steps don't hard-code it.
    food_trade["year"] = year
    food_trade["year"] = food_trade["year"].copy_metadata(food_trade["value"])
    # Carry the FAO item code as a dimension so downstream steps get the display->code
    # mapping from the data itself, rather than re-reading the curated items config.
    display_to_code = {display: code for code, display in code_to_display.items()}
    food_trade["item_code"] = food_trade["item"].map(display_to_code).astype(int)

    return food_trade.format(keys=["exporter", "importer", "item", "item_code"], short_name=paths.short_name)


def run() -> None:
    #
    # Load inputs.
    #
    ds_tm = paths.load_dataset("faostat_tm")
    tb_tm = ds_tm.read("faostat_tm", safe_types=False)
    ds_scl = paths.load_dataset("faostat_scl")
    tb_scl = ds_scl.read("faostat_scl", safe_types=False)

    #
    # Process data.
    #
    tb = build_food_trade_table(tb_tm, tb_scl)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
