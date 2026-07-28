"""Garden step for the FAOSTAT food-trade Sankey viz.

Builds the slice of bilateral trade flows from the trade matrix:

  exporter (str)               — exporting country (OWID-harmonized)
  importer (str)               — importing country (OWID-harmonized)
  item     (str)               — viz-display item (see food_trade.items.yaml)
  value    (float, tonnes)     — bilateral A→B trade flow in tonnes

The display items shown in the dropdown are curated in
`food_trade.items.yaml`. Each entry maps one or more FAO commodity item codes
to their names. Most items have a single code, so the rollup is a direct integer-code
filter against `item_code`.

Some items combine several codes: FAO splits some commodities into a primary
product and a mechanically-derived form or preservation state whose trade is
reported separately (beef bone-in + boneless, almonds in-shell + shelled,
milled + broken rice, raw + refined sugar, fresh + dried figs, fresh +
preserved olives), and the primary code alone captures only a fraction of
the traded weight. For these we sum the trade of all the item's codes to
recover the full bilateral flow. Summing the *trade* does not double-count (the
codes are distinct shipments under distinct customs headings). Extracted or
milled derivatives (flour, starch, oil, juice) are never folded into the crop;
they stay separate items. A combined item is identified in the output by
`100000 + its first code`, so its id is never mistaken for a single FAO
commodity.

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

import yaml
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Offset added to a combined item's first code to form its id (e.g. beef 867 -> 100867). FAO item
# codes top out in the low thousands, so the 100000+ range can never collide with a real code, and
# the id stays an integer (no change needed downstream / in the viz). See module docstring.
COMBINED_ID_OFFSET = 100_000


def parse_items_config(config: dict) -> list[dict]:
    """Normalise the items config into one dict per item:

        {"display": str, "codes": {code: fao_name, ...}, "id": int, "combined": bool}

    `codes` preserves file order; the first code is the representative one. `id` is that code for
    single-code items, or `COMBINED_ID_OFFSET + first_code` for combined ones.
    """
    assert isinstance(config, dict) and "items" in config, "items config must be a mapping with an 'items' key"
    raw = config["items"]
    assert isinstance(raw, list) and raw, "config['items'] must be a non-empty list"

    items = []
    for entry in raw:
        assert isinstance(entry, dict) and entry.keys() == {"display", "item_codes"}, (
            f"items entry must have exactly 'display' and 'item_codes': {entry!r}"
        )
        codes = {int(code): name for code, name in entry["item_codes"].items()}
        assert codes, f"item_codes is empty for {entry['display']!r}"
        first = next(iter(codes))
        items.append(
            {
                "display": entry["display"],
                "codes": codes,
                "id": first + COMBINED_ID_OFFSET if len(codes) > 1 else first,
                "combined": len(codes) > 1,
            }
        )
    return items


def sanity_check_items_config(items: list[dict], tm_items: dict[int, str]) -> None:
    """Validate the parsed items against the TM snapshot.

    `tm_items` maps each item code present in the year's quantity (tonnes) trade flows to its
    FAO item name.

    Checks:
      1. display names are unique, and no code is reused across items.
      2. Each code appears among the year's quantity (tonnes) trade flows (catches typos, removed
         codes, or items with no traded quantity that we couldn't show anyway).
      3. Each code's FAO item name still matches the curated one (catches FAO silently reassigning
         or renaming a code to a different commodity).
    """
    displays = [it["display"] for it in items]
    dupes = sorted({d for d in displays if displays.count(d) > 1})
    assert not dupes, f"Duplicate display names in items config: {dupes}"

    code_to_fao = {code: name for it in items for code, name in it["codes"].items()}
    n_codes = sum(len(it["codes"]) for it in items)
    assert len(code_to_fao) == n_codes, "An item code is used by more than one item."

    missing_codes = sorted(code for code in code_to_fao if code not in tm_items)
    assert not missing_codes, (
        f"{len(missing_codes)} item code(s) have no quantity (tonnes) trade in the TM snapshot "
        f"for the selected year: {missing_codes[:10]}"
    )

    renamed = [
        f"{code}: expected {fao!r}, TM has {tm_items[code]!r}"
        for code, fao in code_to_fao.items()
        if tm_items[code] != fao
    ]
    assert not renamed, (
        "FAO item name no longer matches the curated name for: "
        + "; ".join(renamed)
        + ". Verify each code still refers to the intended commodity before updating the name."
    )


def build_food_trade_table(tb_tm: Table) -> Table:
    """Reshape the trade matrix into the viz-ready slice of bilateral flows."""
    # Pick the latest well-covered year: the latest one whose distinct-reporter count is at
    # least 90% of the series maximum, i.e. not the partially reported tail year. We count
    # reporters rather than rows so a genuine trade contraction can't look like low coverage.
    reporters_per_year = tb_tm.groupby("year", observed=True)["reporter_country"].nunique()
    year = int(reporters_per_year[reporters_per_year >= 0.9 * reporters_per_year.max()].index.max())

    # Load and normalise the curated items.
    with open(paths.side_file("food_trade.items.yaml")) as f:
        items = parse_items_config(yaml.safe_load(f))
    code_to_display = {code: it["display"] for it in items for code in it["codes"]}

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
    sanity_check_items_config(items, tm_items=tm_items)

    trade_flows["item"] = trade_flows["item_code"].map(code_to_display)
    # Collapse to one flow per (reporter, partner, item, element): for items that combine several
    # codes (beef = 867 + 870, rice = 31 + 32 + 28, ...) this sums them into a single bilateral
    # flow; for single-code items it is a no-op.
    trade_flows = trade_flows.groupby(
        ["reporter_country", "partner_country", "item", "element", "year"], observed=True, as_index=False
    )["value"].sum()

    # 2) Join the directional reports into one row per (exporter, importer, item, year).
    export_reports = trade_flows.loc[
        trade_flows["element"] == "Export quantity", ["reporter_country", "partner_country", "item", "year", "value"]
    ].rename(columns={"reporter_country": "exporter", "partner_country": "importer", "value": "value_exporter"})
    import_reports = trade_flows.loc[
        trade_flows["element"] == "Import quantity", ["reporter_country", "partner_country", "item", "year", "value"]
    ].rename(columns={"reporter_country": "importer", "partner_country": "exporter", "value": "value_importer"})

    bilateral = pr.merge(export_reports, import_reports, on=["exporter", "importer", "item", "year"], how="outer")
    # Default to the importer-reported value; fall back to the exporter-reported value
    # only when the importer doesn't report. See docstring for the rationale.
    bilateral["value"] = bilateral["value_importer"].fillna(bilateral["value_exporter"])
    bilateral = bilateral.dropna(subset=["value"])
    bilateral = bilateral[bilateral["value"] > 0]

    food_trade = bilateral[["exporter", "importer", "item", "year", "value"]]
    food_trade = food_trade.sort_values(["exporter", "importer", "item", "year"]).reset_index(drop=True)
    # Carry the item id as a dimension so downstream steps get the display->id mapping from the
    # data itself, rather than re-reading the curated items config. Single-code items use their FAO
    # code; combined items use 100000 + their first code (see parse_items_config / module docstring).
    display_to_id = {it["display"]: it["id"] for it in items}
    food_trade["item_code"] = food_trade["item"].map(display_to_id).astype(int)

    return food_trade.format(keys=["exporter", "importer", "item", "item_code", "year"], short_name=paths.short_name)


def run() -> None:
    #
    # Load inputs.
    #
    ds_tm = paths.load_dataset("faostat_tm")
    tb_tm = ds_tm.read("faostat_tm", safe_types=False)

    #
    # Process data.
    #
    tb = build_food_trade_table(tb_tm)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
