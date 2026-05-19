"""Garden step for the FAOSTAT food-trade Sankey viz.

Builds the slim, viz-ready slice of bilateral trade flows from the trade
matrix (`faostat_tm`), with two context columns from QCL Production:

  exporter (str)               — exporting country (OWID-harmonized)
  importer (str)               — importing country (OWID-harmonized)
  item     (str)               — viz-display item (see food_trade.items.yaml)
  value    (float, tonnes)     — bilateral A→B trade flow in tonnes
  exporter_production (float)  — exporter's QCL Production tonnes of that item
                                 (NaN if QCL has no Production row for it)
  importer_supply (float)      — importer's apparent domestic supply tonnes,
                                 computed as
                                     Production + Total imports − Total exports
                                 using the importer's own TM reports. NaN
                                 when QCL has no Production figure for the
                                 (importer, item) pair.

The display items shown in the dropdown are curated in
`food_trade.items.yaml`. Each entry names a single FAO commodity item code
(the same codebook used by both TM and QCL), so the rollup is a direct
integer-code filter against `item_code`.

For each (exporter, importer, item) the trade matrix typically has two
reports — one from each side — that can disagree. When both sides report,
we take the **geometric mean** of the two quantities (√(exp × imp));
this treats over- and under-reporting symmetrically in log space, which
is the natural choice for heavy-tailed trade flows. When only one side
reports, we use that side's number. The notebook at
docs/analyses/food_trade/food_trade.ipynb justifies this choice and
contrasts it with the academic alternative (CEPII-BACI reliability
weights, Gaulier & Zignago 2010).

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


def _qcl_production_by_country_and_code(tb_qcl: Table, year: int) -> pd.DataFrame:
    """Return QCL Production tonnes for `year`, keyed on (country, item_code).

    QCL uses the same FAO commodity item codebook as TM (codes 1-1296), so the
    join with TM is a direct integer-code lookup. We keep rows with
    `element == "Production"` and `unit_short_name == "t"`; the few non-tonne
    rows (live animals reported as `"head"`) are filtered out because the YAML
    deliberately surfaces only crops.

    Country names come from QCL (OWID-harmonized by the broader FAOSTAT
    pipeline). They match our TM `reporter_country` names (also OWID-harmonized,
    via `faostat_tm.countries.json`): 209 of 221 TM countries also appear in
    QCL; the 12 unmapped ones are tiny territories with no agricultural output."""
    prod = tb_qcl[
        (tb_qcl["element"] == "Production") & (tb_qcl["year"] == year) & (tb_qcl["unit_short_name"] == "t")
    ].copy()
    # QCL stores `item_code` as a zero-padded string ("00000015"); cast to int.
    prod["item_code"] = prod["item_code"].astype(int)
    prod["country"] = prod["country"].astype(str)
    out = pd.DataFrame(prod[["country", "item_code", "value"]]).rename(columns={"value": "production"})
    # If QCL has multiple rows per (country, item_code) — e.g. mainland +
    # aggregate ambiguity — sum them deterministically.
    return out.groupby(["country", "item_code"], as_index=False, observed=True)[["production"]].sum()


def build_food_trade_table(tb_tm: Table, tb_qcl: Table) -> Table:
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

    # 2) QCL Production by (country, item), restricted to curated codes.
    qcl_prod = _qcl_production_by_country_and_code(tb_qcl, YEAR)
    qcl_prod = qcl_prod[qcl_prod["item_code"].isin(code_to_display)].copy()
    qcl_prod["item"] = qcl_prod["item_code"].map(code_to_display)
    production = qcl_prod[["country", "item", "production"]]

    # 3) Per-(country, item) total Import / Export quantity from each country's
    #    own reports — used as the trade-flow components of apparent supply.
    totals = pd.DataFrame(
        qty.groupby(["reporter_country", "item", "element"], observed=True)["value"]
        .sum()
        .unstack("element", fill_value=0.0)
        .reset_index()
    ).rename(
        columns={
            "reporter_country": "country",
            "Import quantity": "country_imports",
            "Export quantity": "country_exports",
        }
    )
    for col in ("country_imports", "country_exports"):
        if col not in totals.columns:
            totals[col] = 0.0

    # Apparent domestic supply = Production + Imports − Exports. Production is
    # required (a missing QCL row leaves `importer_supply` NaN); missing import
    # / export flows are treated as zero (a country that didn't report a flow
    # for an item simply didn't trade it).
    supply = totals.merge(production, on=["country", "item"], how="left")
    supply["importer_supply"] = (
        supply["production"] + supply["country_imports"].fillna(0) - supply["country_exports"].fillna(0)
    )
    supply = supply.rename(columns={"country": "importer"})[["importer", "item", "importer_supply"]]
    exporter_production = production.rename(columns={"country": "exporter", "production": "exporter_production"})

    # 4) Join the directional reports into one row per (exporter, importer, item).
    exp_side = qty.loc[
        qty["element"] == "Export quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(columns={"reporter_country": "exporter", "partner_country": "importer", "value": "value_exporter"})
    imp_side = qty.loc[
        qty["element"] == "Import quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(columns={"reporter_country": "importer", "partner_country": "exporter", "value": "value_importer"})

    merged = exp_side.merge(imp_side, on=["exporter", "importer", "item"], how="outer")
    # When both sides report, take the geometric mean of the two quantities (this
    # treats over- and under-reporting symmetrically in log space, which is the
    # natural choice for heavy-tailed trade flows). When only one side reports,
    # use that side's number.
    merged["value"] = merged["value_importer"].fillna(merged["value_exporter"])
    both = merged["value_exporter"].notna() & merged["value_importer"].notna()
    merged.loc[both, "value"] = (
        merged.loc[both, "value_exporter"] * merged.loc[both, "value_importer"]
    ) ** 0.5
    merged = merged.dropna(subset=["value"])
    merged = merged[merged["value"] > 0]

    out = pd.DataFrame(merged[["exporter", "importer", "item", "value"]])
    out = out.merge(exporter_production, on=["exporter", "item"], how="left")
    out = out.merge(supply, on=["importer", "item"], how="left")
    out = out.sort_values(["exporter", "importer", "item"]).reset_index(drop=True)

    # Wrap as an owid Table and set the natural key as index.
    tb_out = Table(out, short_name=paths.short_name, underscore=False)
    tb_out = tb_out.format(keys=["exporter", "importer", "item"], short_name=paths.short_name)
    return tb_out


def run() -> None:
    #
    # Load inputs.
    #
    ds_tm = paths.load_dataset("faostat_tm")
    tb_tm = ds_tm.read("faostat_tm", safe_types=False)
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl", safe_types=False)

    #
    # Process data.
    #
    tb = build_food_trade_table(tb_tm, tb_qcl)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
