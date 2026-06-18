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
`food_trade.items.yaml`. Each entry maps one or more FAO commodity item codes
(the same codebook used by both TM and SCL) to their names. Most items have a
single code, so the rollup is a direct integer-code filter against `item_code`.

A few items combine several codes: FAO splits some commodities into a primary
product and a mechanically-derived form whose trade is reported separately
(beef bone-in + boneless, almonds in-shell + shelled, milled + broken rice,
raw + refined sugar), and the primary code alone captures only a fraction of
the traded weight. For these we sum the trade of all the item's codes to
recover the full bilateral flow. Summing the *trade* does not double-count (the
codes are distinct shipments under distinct customs headings), but we drop the
Production and domestic-supply context: the derived form is processed from the
primary, so summing their production would double-count, and the two are on
incompatible weight bases anyway. A combined item is identified in the output
by `100000 + its first code`, so its id is never mistaken for a single FAO
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

import pandas as pd
import yaml
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Domestic supply is published only where SCL's imports corroborate the trade we observe: SCL's
# recorded imports must cover at least this share of the observed inbound flows, otherwise SCL has
# under-recorded imports and the supply is understated, so we blank it. 0.9 matches the measurement
# noise floor — well-reporting countries agree with the observed trade within ~10%, so a larger gap
# signals a real hole rather than normal CIF/FOB / timing / classification wobble.
MIN_IMPORT_COVERAGE = 0.9

# Regression guard for the import gate: at MIN_IMPORT_COVERAGE the trade matrix corroborates the large
# majority of supplies (~83% in 2023), so blanking should stay well under this share. A larger share
# means SCL imports and observed trade have diverged unexpectedly (bad data or a logic slip), and the
# build should fail rather than ship a hollowed-out supply column.
MAX_SUPPLY_BLANKED_SHARE = 0.20

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


def build_food_trade_table(tb_tm: Table, tb_scl: Table) -> Table:
    """Reshape the trade matrix into the viz-ready slice with Production and
    apparent domestic supply context."""
    # Pick the latest well-covered year: the latest one whose distinct-reporter count is at
    # least 90% of the series maximum, i.e. not the partially reported tail year. We count
    # reporters rather than rows so a genuine trade contraction can't look like low coverage.
    reporters_per_year = tb_tm.groupby("year", observed=True)["reporter_country"].nunique()
    year = int(reporters_per_year[reporters_per_year >= 0.9 * reporters_per_year.max()].index.max())

    # Load and normalise the curated items.
    with open(paths.side_file("food_trade.items.yaml")) as f:
        items = parse_items_config(yaml.safe_load(f))
    code_to_display = {code: it["display"] for it in items for code in it["codes"]}
    # Single-code items carry Production/supply; combined items don't (see module docstring).
    supply_codes = {code for it in items if not it["combined"] for code in it["codes"]}

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

    # 2) Build per-(country, item) Production and apparent domestic supply from SCL.
    #    Supply follows the FBS identity documented in the module docstring; all four
    #    components are SCL quantities in tonnes, restricted to the single-code items. Combined
    #    items are excluded: their codes are on incompatible weight bases and the derived form is
    #    processed from the primary, so production/supply can't be summed (see module docstring).
    components = ["Production", "Import quantity", "Export quantity", "Stock Variation"]
    scl = tb_scl[
        (tb_scl["year"] == year) & (tb_scl["unit_short_name"] == "t") & tb_scl["element"].isin(components)
    ].copy()
    # SCL stores item_code as zero-padded strings ("00000015"); convert to int to join with TM.
    scl["item_code"] = scl["item_code"].astype(str).astype(int)
    scl["country"] = scl["country"].astype(str)
    scl["element"] = scl["element"].astype(str)
    scl = scl[scl["item_code"].isin(supply_codes)]

    # Pivot the four components into one column each, keyed on (country, item_code, year). We
    # pivot rather than groupby-sum so that a duplicate (country, item_code, year, element) row
    # raises instead of being silently summed: SCL reports one value per key, so duplicates
    # would be a data error, not something to aggregate. join_column_levels_with moves the index
    # back to columns and restores each component column's value metadata.
    supply_context = scl.pivot(
        index=["country", "item_code", "year"], columns="element", values="value", join_column_levels_with=""
    )
    # Every component should be present for at least some (country, item); a whole component
    # missing would mean SCL dropped it for the year, which we must not silently treat as 0.
    missing_components = [c for c in components if c not in supply_context.columns]
    assert not missing_components, f"SCL is missing entire component(s) {missing_components} in {year}."
    supply_context["supply"] = (
        supply_context["Production"].fillna(0)
        + supply_context["Import quantity"].fillna(0)
        - supply_context["Export quantity"].fillna(0)
        - supply_context["Stock Variation"].fillna(0)
    )
    # A non-positive supply can't be the denominator of an import share, so treat it as missing:
    # negative means the FBS components don't reconcile (re-exports not captured by stock
    # variation, timing mismatches, primary-equivalent aggregation), and zero is the signature of
    # a (country, item) built entirely from blank components. Either way the ratio should be
    # undefined, not infinite or misleadingly small. Where SCL omits a positive component (e.g. an
    # unrecorded import) supply stays positive but understated; we keep SCL's figure as-is rather
    # than patch it from TM, which we treat as a separate, un-reconciled source.
    supply_context.loc[supply_context["supply"] <= 0, "supply"] = pd.NA
    supply_context["item"] = supply_context["item_code"].map(code_to_display)

    # Exporter context column: only emit `exporter_production` for countries with
    # an SCL Production figure (we don't want to imply "0 production" for absent rows).
    exporter_production = (
        supply_context[["country", "item", "year", "Production"]]
        .dropna(subset=["Production"])
        .rename(columns={"country": "exporter", "Production": "exporter_production"})
    )

    # 3) Join the directional reports into one row per (exporter, importer, item, year).
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

    # Importer context column: the FBS-identity supply per (importer, item), kept only where the
    # trade matrix corroborates it. SCL's import leg is importer-sourced (see module docstring), so
    # when SCL records far fewer imports than the inbound flows we observe, the supply is understated
    # and we blank it: keep supply only where SCL's imports cover at least MIN_IMPORT_COVERAGE of the
    # observed inbound trade (or there is no observed inbound to corroborate). TM only decides whether
    # to trust SCL's figure here; it never changes it.
    observed_inbound = (
        bilateral.groupby(["importer", "item", "year"], observed=True)["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": "observed_inbound"})
    )
    importer_supply = supply_context[["country", "item", "year", "supply", "Import quantity"]].rename(
        columns={"country": "importer", "supply": "importer_supply", "Import quantity": "scl_import"}
    )
    importer_supply = pr.merge(importer_supply, observed_inbound, on=["importer", "item", "year"], how="left")
    scl_import = importer_supply["scl_import"].fillna(0).to_numpy(dtype="float64")
    observed = importer_supply["observed_inbound"].fillna(0).to_numpy(dtype="float64")
    uncorroborated = scl_import < MIN_IMPORT_COVERAGE * observed
    has_supply = importer_supply["importer_supply"].notna().to_numpy()
    n_supply = int(has_supply.sum())
    blanked_share = int((uncorroborated & has_supply).sum()) / n_supply if n_supply else 0.0
    assert blanked_share <= MAX_SUPPLY_BLANKED_SHARE, (
        f"Import gate blanked {blanked_share:.0%} of domestic-supply values (> {MAX_SUPPLY_BLANKED_SHARE:.0%}); "
        "SCL imports and observed trade have diverged unexpectedly — investigate before trusting the output."
    )
    importer_supply.loc[uncorroborated, "importer_supply"] = pd.NA
    importer_supply = importer_supply[["importer", "item", "year", "importer_supply"]]

    food_trade = bilateral[["exporter", "importer", "item", "year", "value"]]
    food_trade = pr.merge(food_trade, exporter_production, on=["exporter", "item", "year"], how="left")
    food_trade = pr.merge(food_trade, importer_supply, on=["importer", "item", "year"], how="left")
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
