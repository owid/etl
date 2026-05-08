"""Reshape per-source child labor tables to a unified ``(<categories>, year, number, share)`` schema."""

import numpy as np
from owid.catalog import Table
from owid.catalog.tables import concat as table_concat

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Country to assign to each per-source table.
# Tables that split rows across multiple countries override this in their reshaper.
TABLE_COUNTRIES: dict[str, str] = {
    "us_long": "United States",
    "belgium": "Belgium",
    "portugal": "Portugal",
    "denmark": "Denmark",
    "italy": "Italy",
    "sweden": "Sweden",
    "us_carter_sutch": "United States",
    "portugal_goulart_bedi": "Portugal",
    "england_wales_scotland": None,  # set per row in the reshaper
    "england_wales": "England and Wales",
    "japan": "Japan",
}

# Index columns per garden table (categories + year + country).
TABLE_INDICES: dict[str, list[str]] = {
    "us_long": ["country", "sex", "age_group", "source", "year"],
    "belgium": ["country", "age", "year"],
    "portugal": ["country", "sector", "sex", "age_group", "year"],
    "denmark": ["country", "age_group", "sector", "year"],
    "italy": ["country", "sex", "sector", "year"],
    "sweden": ["country", "sector", "year"],
    "us_carter_sutch": ["country", "sex", "age_group", "year"],
    "portugal_goulart_bedi": ["country", "measure", "age_group", "bound", "year"],
    "england_wales_scotland": ["country", "sex", "occupation", "year"],
    "england_wales": ["country", "age_group", "year"],
    "japan": ["country", "gender", "age_group", "year"],
}

ITALY_SECTOR_LABELS = {
    "agr": "Agriculture",
    "build": "Building",
    "manu": "Manufacturing",
    "trade": "Trade",
    "trans": "Transport and communications",
    "cred": "Credit",
    "pa": "Public administration",
    "all": "All sectors",
}


def _melt_year_prefixed(tb: Table, id_cols: list[str], stubs: list[str]) -> Table:
    """Melt columns of the form ``<stub>_<year>`` (one melt per stub, then merge)."""
    parts = []
    for stub in stubs:
        cols = [c for c in tb.columns if c.startswith(f"{stub}_")]
        part = tb[id_cols + cols].melt(id_vars=id_cols, var_name="year", value_name=stub)
        part["year"] = part["year"].str.removeprefix(f"{stub}_").astype(int)
        parts.append(part)
    out = parts[0]
    for part in parts[1:]:
        out = out.merge(part, on=id_cols + ["year"], how="outer")
    return out


def _reshape_us_long(tb: Table) -> Table:
    # Source is empty for most rows; only "Both, 10 & older" splits into Census/Whelpton.
    tb = tb.copy()
    tb["source"] = tb["source"].astype(object).fillna("Combined")
    out = _melt_year_prefixed(tb, id_cols=["sex", "age_group", "source"], stubs=["millions", "pct"])
    out["number"] = out["millions"].astype(float) * 1_000_000
    out["share"] = out["pct"].astype(float)
    return out[["sex", "age_group", "source", "year", "number", "share"]]


def _reshape_belgium(tb: Table) -> Table:
    out = _melt_year_prefixed(tb, id_cols=["age"], stubs=["number", "pct"])
    out = out.rename(columns={"pct": "share"})
    return out[["age", "year", "number", "share"]]


def _reshape_portugal(tb: Table) -> Table:
    age_to_label = {"10_14": "10-14", "15_19": "15-19"}

    number_parts = []
    for age_key, age_label in age_to_label.items():
        for sex in ["boys", "girls", "total"]:
            col = f"{sex}_{age_key}"
            part = tb[["sector", "year", col]].rename(columns={col: "number"})
            part["sex"] = sex
            part["age_group"] = age_label
            number_parts.append(part)
    numbers = table_concat(number_parts, ignore_index=True)

    share_parts = []
    for age_key, age_label in age_to_label.items():
        col = f"rate_{age_key}"
        part = tb[["sector", "year", col]].rename(columns={col: "share"})
        part["sex"] = "total"
        part["age_group"] = age_label
        share_parts.append(part)
    shares = table_concat(share_parts, ignore_index=True)

    out = numbers.merge(shares, on=["sector", "year", "sex", "age_group"], how="outer")
    return out[["sector", "sex", "age_group", "year", "number", "share"]]


def _reshape_denmark(tb: Table) -> Table:
    rows = []
    for age_group in ["children", "young"]:
        for sector in ["textile", "industry"]:
            n_col = f"{age_group}_{sector}_n"
            p_col = f"{age_group}_{sector}_pct"
            part = tb[["year", n_col, p_col]].rename(columns={n_col: "number", p_col: "share"})
            part["age_group"] = age_group
            part["sector"] = sector
            rows.append(part)
    out = table_concat(rows, ignore_index=True)
    return out[["age_group", "sector", "year", "number", "share"]]


def _reshape_italy(tb: Table) -> Table:
    sectors = list(ITALY_SECTOR_LABELS)
    out = tb.melt(id_vars=["group", "year"], value_vars=sectors, var_name="sector", value_name="share")
    out["sector"] = out["sector"].map(ITALY_SECTOR_LABELS)
    out = out.rename(columns={"group": "sex"})
    out["number"] = np.nan
    return out[["sex", "sector", "year", "number", "share"]]


def _reshape_sweden(tb: Table) -> Table:
    rows = []
    for sector in ["glass", "tobacco", "cotton", "matches"]:
        n_col = f"{sector}_number"
        p_col = f"{sector}_pct"
        part = tb[["year", n_col, p_col]].rename(columns={n_col: "number", p_col: "share"})
        part["sector"] = sector
        rows.append(part)
    out = table_concat(rows, ignore_index=True)
    return out[["sector", "year", "number", "share"]]


def _reshape_us_carter_sutch(tb: Table) -> Table:
    out = tb.melt(
        id_vars=["year"],
        value_vars=["males_10_15", "females_10_15"],
        var_name="sex",
        value_name="share",
    )
    out["sex"] = out["sex"].str.removesuffix("_10_15")
    out["age_group"] = "10-15"
    out["number"] = np.nan
    return out[["sex", "age_group", "year", "number", "share"]]


def _reshape_portugal_goulart_bedi(tb: Table) -> Table:
    rows = []
    for measure in ["employed", "occupied", "activity", "unemployment"]:
        for age_key, age_label in [("10_14", "10-14"), ("15_19", "15-19")]:
            col = f"{measure}_{age_key}"
            part = tb[["year", col]].rename(columns={col: "share"})
            part["measure"] = measure
            part["age_group"] = age_label
            part["bound"] = "central"
            rows.append(part)
    for bound in ["lower", "upper"]:
        col = f"paid_10_19_{bound}"
        part = tb[["year", col]].rename(columns={col: "share"})
        part["measure"] = "paid"
        part["age_group"] = "10-19"
        part["bound"] = bound
        rows.append(part)
    out = table_concat(rows, ignore_index=True)
    out["number"] = np.nan
    return out[["measure", "age_group", "bound", "year", "number", "share"]]


def _reshape_england_wales_scotland(tb: Table) -> Table:
    rows = []
    for region_prefix, country in [("england_wales", "England and Wales"), ("scotland", "Scotland")]:
        for sex in ["males", "females"]:
            col = f"{region_prefix}_{sex}"
            part = tb[["occupation", "year", col]].rename(columns={col: "share"})
            part["country"] = country
            part["sex"] = sex
            rows.append(part)
    out = table_concat(rows, ignore_index=True)
    out["number"] = np.nan
    return out[["country", "sex", "occupation", "year", "number", "share"]]


def _reshape_england_wales(tb: Table) -> Table:
    year_cols = [c for c in tb.columns if c.startswith("_") and c[1:].isdigit()]
    out = tb.melt(id_vars=["age_group"], value_vars=year_cols, var_name="year", value_name="share")
    out["year"] = out["year"].str.removeprefix("_").astype(int)
    out["number"] = np.nan
    return out[["age_group", "year", "number", "share"]]


def _reshape_japan(tb: Table) -> Table:
    rows = []
    for cutoff in ["12", "14", "20"]:
        col = f"pct_under_{cutoff}"
        part = tb[["gender", "year", col]].rename(columns={col: "share"})
        part["age_group"] = f"Under {cutoff}"
        part["number"] = np.nan
        rows.append(part)
    total = tb[["gender", "year", "total_workforce_all_ages"]].rename(columns={"total_workforce_all_ages": "number"})
    total["age_group"] = "All ages"
    total["share"] = np.nan
    rows.append(total)
    out = table_concat(rows, ignore_index=True)
    return out[["gender", "age_group", "year", "number", "share"]]


RESHAPERS = {
    "us_long": _reshape_us_long,
    "belgium": _reshape_belgium,
    "portugal": _reshape_portugal,
    "denmark": _reshape_denmark,
    "italy": _reshape_italy,
    "sweden": _reshape_sweden,
    "us_carter_sutch": _reshape_us_carter_sutch,
    "portugal_goulart_bedi": _reshape_portugal_goulart_bedi,
    "england_wales_scotland": _reshape_england_wales_scotland,
    "england_wales": _reshape_england_wales,
    "japan": _reshape_japan,
}


def _propagate_origins(tb: Table) -> None:
    """Make sure ``number`` and ``share`` columns have origins (in place)."""
    donor = next(
        (c for c in tb.columns if c not in ("number", "share") and getattr(tb[c].metadata, "origins", None)),
        None,
    )
    if donor is None:
        return
    for col in ("number", "share"):
        if col in tb.columns and not getattr(tb[col].metadata, "origins", None):
            tb[col].copy_metadata(tb[donor], inplace=True)


def run() -> None:
    ds_meadow = paths.load_dataset("child_labor_long_run")

    tables = []
    for short_name, country in TABLE_COUNTRIES.items():
        tb = ds_meadow[short_name].reset_index()
        tb = RESHAPERS[short_name](tb)
        if country is not None:
            tb["country"] = country
        # Drop number/share if the source has no values for it.
        for col in ("number", "share"):
            if col in tb.columns and tb[col].isna().all():
                tb = tb.drop(columns=col)
        _propagate_origins(tb)
        tb = tb.format(TABLE_INDICES[short_name], short_name=short_name)
        tables.append(tb)

    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)
    ds_garden.save()
