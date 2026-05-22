"""Garden step for the FAOSTAT food-trade Sankey viz.

Builds the slim, viz-ready slice of bilateral trade flows from the trade
matrix (`faostat_tm`), with two context columns from SCL Production:

  exporter (str)               — exporting country (OWID-harmonized)
  importer (str)               — importing country (OWID-harmonized)
  item     (str)               — viz-display item (see food_trade.items.yaml)
  value    (float, tonnes)     — bilateral A→B trade flow in tonnes
  exporter_production (float)  — exporter's SCL Production tonnes of that item
                                 (NaN if SCL has no Production row for it)
  importer_supply (float)      — importer's domestic supply in tonnes,
                                 computed from SCL via the FAOSTAT Food
                                 Balance Sheet identity:
                                     Production + Imports − Exports − Stock Variation
                                 All four components come from SCL. Stock
                                 Variation in SCL is `Closing − Opening`
                                 (positive when stocks accumulated during
                                 the year, negative when drawn down) — so
                                 it is *subtracted* from the supply, as
                                 accumulated stocks don't reach domestic
                                 use. NaN when SCL has no row for the
                                 (importer, item) pair at all, or when the
                                 FBS identity returns a negative (a
                                 data-inconsistency signal).

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

from pathlib import Path

import pandas as pd
import yaml
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Year exported to the viz. Hard-coded on purpose: when FAOSTAT publishes a
# new release, the assertion below will fail and force a deliberate bump
# rather than silently shifting the exported slice forward.
YEAR = 2023

# Sibling config file. Name starts with "food_trade" so ETL change-detection
# picks it up automatically (see etl/steps/__init__.py:_step_files).
ITEMS_CONFIG_PATH = Path(__file__).parent / "food_trade.items.yaml"


# ─────────────────────────────────────────────────────────────────────────────
# YAML config loading & validation
# ─────────────────────────────────────────────────────────────────────────────


def _load_items_config() -> dict:
    """Load the curated items config from food_trade.items.yaml."""
    with open(ITEMS_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _sanity_check_items_config(config: dict, tm_codes_in_data: set[int]) -> None:
    """Validate the items config structurally and against the TM snapshot.

    Checks:
      1. Required top-level shape: a single 'items' list.
      2. Each items entry has required fields (display, item_code).
      3. display names are unique.
      4. Each item_code exists in the TM snapshot (catches typos / removed codes).
    """
    assert isinstance(config, dict) and "items" in config, "items config must be a mapping with an 'items' key"
    items = config["items"]
    assert isinstance(items, list) and items, "config['items'] must be a non-empty list"

    required = {"display", "item_code"}
    for entry in items:
        missing = required - entry.keys()
        assert not missing, f"items entry missing keys {sorted(missing)}: {entry}"

    displays = [e["display"] for e in items]
    dupes = sorted({d for d in displays if displays.count(d) > 1})
    assert not dupes, f"Duplicate display names in items config: {dupes}"

    missing_codes = sorted(int(e["item_code"]) for e in items if int(e["item_code"]) not in tm_codes_in_data)
    assert not missing_codes, (
        f"{len(missing_codes)} item_code(s) not found in TM snapshot for {YEAR}: {missing_codes[:10]}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _assert_year_is_latest_well_covered(tb: Table, year: int) -> None:
    """Assert that `year` is the latest year whose number of distinct reporting
    countries is at least 90% of the series maximum — i.e. the latest year
    that is not the partially reported tail year.

    We use distinct reporters rather than row count because row count
    conflates "fewer countries submitted" with "less trade activity"
    (cf. plot_coverage() in the faostat_tm garden step). A real trade
    contraction could spuriously trip a row-count threshold; a reporter
    headcount drop is unambiguous coverage signal.

    Fails loudly when FAOSTAT extends the matrix so we can update the
    `YEAR` constant deliberately."""
    reporters_per_year = tb.groupby("year", observed=True)["reporter_country"].nunique()
    threshold = 0.9 * reporters_per_year.max()
    latest_well_covered = int(reporters_per_year[reporters_per_year >= threshold].index.max())
    assert latest_well_covered == year, (
        f"YEAR is hard-coded to {year}, but the latest well-covered year (by reporter "
        f"count) in the data is {latest_well_covered}. Bump YEAR (and re-run the viz) deliberately."
    )


def _scl_supply_by_country_and_code(tb_scl: Table, year: int) -> pd.DataFrame:
    """Return SCL-derived Production and Domestic Supply for `year`, keyed on (country, item_code).

    Domestic Supply follows the FAOSTAT Food Balance Sheet identity:

        Domestic Supply = Production + Imports − Exports − Stock Variation

    All four components come from SCL (in tonnes). Stock Variation in
    FAOSTAT SCL is defined as `Closing − Opening` (positive when stocks
    accumulate during the year, negative when stocks are drawn down) —
    verified empirically against the `Opening stocks` element. It is
    therefore *subtracted* from the supply identity: accumulated stocks
    don't reach domestic use, while drawn-down stocks do.

    Missing components are treated as 0 (a country that didn't report a
    flow or didn't change stocks for an item simply had none). A
    (country, item) pair absent from SCL entirely is absent from the
    output, and will produce NaN in the downstream merge.

    SCL uses the same FAO commodity item codebook as TM (codes 1-1296), so
    the join with TM is a direct integer-code lookup."""
    elements = ["Production", "Import quantity", "Export quantity", "Stock Variation"]
    sub = tb_scl[
        (tb_scl["year"] == year) & (tb_scl["unit_short_name"] == "t") & tb_scl["element"].isin(elements)
    ].copy()
    # SCL stores `item_code` as a categorical of zero-padded strings ("00000015");
    # convert via str to int so it joins cleanly with TM's integer item codes.
    sub["item_code"] = sub["item_code"].astype(str).astype(int)
    sub["country"] = sub["country"].astype(str)
    sub["element"] = sub["element"].astype(str)

    wide = pd.DataFrame(
        sub.groupby(["country", "item_code", "element"], observed=True)["value"].sum().unstack("element").reset_index()
    )
    # Ensure all four element columns exist even if SCL didn't have any rows for one.
    for elem in elements:
        if elem not in wide.columns:
            wide[elem] = pd.NA

    wide["supply"] = (
        wide["Production"].fillna(0)
        + wide["Import quantity"].fillna(0)
        - wide["Export quantity"].fillna(0)
        - wide["Stock Variation"].fillna(0)
    )
    # Negative supply is not physically meaningful — it signals that FBS components
    # don't reconcile for that (country, item) (re-exports not fully captured by
    # Stock Variation, timing mismatches, primary-equivalent aggregation, etc.).
    # NaN it out rather than clipping to 0, so the downstream "imports as a share
    # of supply" ratio is undefined for these pairs instead of misleadingly zero.
    wide.loc[wide["supply"] < 0, "supply"] = pd.NA
    return wide[["country", "item_code", "Production", "supply"]].rename(columns={"Production": "production"})


def build_food_trade_table(tb_tm: Table, tb_scl: Table) -> Table:
    """Reshape the trade matrix into the viz-ready slice with Production and
    apparent domestic supply context."""
    _assert_year_is_latest_well_covered(tb_tm, YEAR)

    # 1) Filter TM to physical quantities in tonnes for the chosen year,
    #    drop self-trade rows, and restrict to the curated item universe.
    qty = tb_tm[
        (tb_tm["year"] == YEAR) & tb_tm["element"].isin(["Export quantity", "Import quantity"]) & (tb_tm["unit"] == "t")
    ].copy()
    qty["reporter_country"] = qty["reporter_country"].astype(str)
    qty["partner_country"] = qty["partner_country"].astype(str)
    qty["item_code"] = qty["item_code"].astype(int)
    qty = qty[qty["reporter_country"] != qty["partner_country"]]

    items_config = _load_items_config()
    _sanity_check_items_config(items_config, tm_codes_in_data=set(qty["item_code"].unique()))

    code_to_display = {int(e["item_code"]): e["display"] for e in items_config["items"]}
    qty = qty[qty["item_code"].isin(code_to_display)].copy()
    qty["item"] = qty["item_code"].map(code_to_display)

    # 2) SCL-derived Production and Domestic Supply by (country, item), restricted to
    #    curated codes. Supply follows the FAOSTAT FBS identity
    #        Production + Imports − Exports + Stock Variation
    #    with every component sourced from SCL (see `_scl_supply_by_country_and_code`).
    scl_ctx = _scl_supply_by_country_and_code(tb_scl, YEAR)
    scl_ctx = scl_ctx[scl_ctx["item_code"].isin(code_to_display)].copy()
    scl_ctx["item"] = scl_ctx["item_code"].map(code_to_display)

    # Exporter context column: only emit `exporter_production` for countries with
    # an SCL Production figure (we don't want to imply "0 production" for absent rows).
    exporter_production = (
        scl_ctx[["country", "item", "production"]]
        .dropna(subset=["production"])
        .rename(columns={"country": "exporter", "production": "exporter_production"})
    )

    # Importer context column: use the FBS-identity supply for every (country, item)
    # row SCL knows about. Pairs entirely absent from SCL will fall out as NaN in
    # the downstream merge.
    supply = scl_ctx[["country", "item", "supply"]].rename(columns={"country": "importer", "supply": "importer_supply"})

    # 4) Join the directional reports into one row per (exporter, importer, item).
    exp_side = qty.loc[
        qty["element"] == "Export quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(columns={"reporter_country": "exporter", "partner_country": "importer", "value": "value_exporter"})
    imp_side = qty.loc[
        qty["element"] == "Import quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(columns={"reporter_country": "importer", "partner_country": "exporter", "value": "value_importer"})

    merged = exp_side.merge(imp_side, on=["exporter", "importer", "item"], how="outer")
    # Default to the importer-reported value; fall back to the exporter-reported value
    # only when the importer doesn't report. See docstring for the rationale.
    merged["value"] = merged["value_importer"].fillna(merged["value_exporter"])
    merged = merged.dropna(subset=["value"])
    merged = merged[merged["value"] > 0]

    out = pd.DataFrame(merged[["exporter", "importer", "item", "value"]])
    out = out.merge(exporter_production, on=["exporter", "item"], how="left")
    out = out.merge(supply, on=["importer", "item"], how="left")
    out = out.sort_values(["exporter", "importer", "item"]).reset_index(drop=True)

    # Wrap as an owid Table and propagate origins to the numeric columns
    # (pandas merges and arithmetic above strip Variable metadata).
    tb_out = Table(out, short_name=paths.short_name, underscore=False)
    tb_out["value"] = tb_out["value"].copy_metadata(tb_tm["value"])
    tb_out["exporter_production"] = tb_out["exporter_production"].copy_metadata(tb_scl["value"])
    tb_out["importer_supply"] = tb_out["importer_supply"].copy_metadata(tb_scl["value"])
    tb_out = tb_out.format(keys=["exporter", "importer", "item"], short_name=paths.short_name)
    return tb_out


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
