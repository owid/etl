"""Scan a built garden dataset for internal red flags: the candidates for the online cross-check.

A candidate generator, not a verdict: every row is a value to look at, and most have a legitimate reason (a war, a
pandemic, a currency redenomination). The reviewer discards those with a stated reason each before spending web
calls on the rest.

Flags, per numeric column:
  EMPTY      the column is all missing
  UNIT       the unit is a percentage but the values look like fractions (max <= 1.5) or run past 150
  SIGN       a negative value in a count-like unit
  OUTLIER    a value more than 6 robust z-scores (median/MAD) from its own series, for series of 8+ points
  BREAK      a jump of x10 or more between consecutive non-zero values (the signature of a unit error)
  COVERAGE   the latest period has under 70% of the previous period's countries
  CONSTANT   more than half of a series repeats the previous period's non-zero value (a source forward-fill?)
  WORLD_SUM  in an additive unit, World differs from the sum of countries by more than 5% in the latest year

A series is one entity and one value of every extra dimension (sex, age, ...). Tables without a country or a time
column get only the EMPTY, UNIT and SIGN checks.

    .venv/bin/python .claude/skills/fact-check-dataset/scripts/anomaly_scan.py <namespace>/<version>/<short_name> \
        [--columns a,b] [--entities "France,World"] [--out anomalies.csv]
"""

import argparse
import csv
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PERCENT_WORDS = ("%", "percent", "share")
COUNT_WORDS = ("people", "number", "deaths", "tonnes", "count")
# Units that add up across countries, and the markers that say a unit is a rate after all.
ADDITIVE_WORDS = ("people", "persons", "deaths", "tonnes", "cases", "births", "number", "count")
RATE_MARKERS = ("per ", "/", "%", "percent", "share", "rate", "ratio", "index", "average", "mean", "median")
MAX_ROWS_PER_FLAG = 20
MAX_CONSTANT_SERIES = 10
MIN_POINTS_FOR_OUTLIERS = 8
MAX_DIMENSION_VALUES = 200


def scan_table(
    tb: pd.DataFrame,
    table_name: str,
    units: dict[str, str],
    countries: set[str] | None = None,
    dims: list[str] | None = None,
) -> list[dict]:
    """Flags for one table. `tb` has its keys as regular columns; `units` maps a column to its unit.

    `dims` are the extra dimensions (sex, age, ...) that, with the entity, identify a series: the table's primary
    key without country and time. When it is not given they are inferred (see `infer_dims`).
    """
    findings: list[dict] = []

    def add(column: str, kind: str, detail: str, period="") -> None:
        findings.append({"table": table_name, "column": column, "kind": kind, "period": period, "detail": detail})

    year_col = "year" if "year" in tb.columns else ("date" if "date" in tb.columns else None)
    has_country = "country" in tb.columns
    keys = (["country"] if has_country else []) + ([year_col] if year_col else [])
    if dims is None:
        dims = infer_dims(tb, keys)
    dims = [d for d in dims if d in tb.columns and d not in keys]
    values = [c for c in tb.columns if c not in keys and c not in dims and _is_value(tb[c])]
    series_keys = (["country"] if has_country else []) + dims

    for column in values:
        unit = (units.get(column) or "").lower()
        s = tb[keys + dims + [column]].dropna(subset=[column])
        if s.empty:
            add(column, "EMPTY", "all-NaN column")
            continue
        largest, smallest = s[column].max(), s[column].min()
        if any(word in unit for word in PERCENT_WORDS):
            if largest <= 1.5:
                add(column, "UNIT", f"unit is % but max={largest:.3g}: fraction stored?")
            if largest > 150:
                add(column, "UNIT", f"% column max={largest:.3g}: can it exceed 100?")
        if smallest < 0 and any(word in unit for word in COUNT_WORDS):
            add(column, "SIGN", f"negative min={smallest:.3g} in count-like unit")
        if not has_country or year_col is None:
            continue

        def label(row) -> str:
            extra = "".join(f", {d}={row[d]}" for d in dims)
            return f"{row['country']}{extra} {row[year_col]}"

        groups = s.groupby(series_keys, observed=True)[column]
        median = groups.transform("median")
        deviation = (s[column] - median).abs()
        mad = deviation.groupby([s[k] for k in series_keys], observed=True).transform("median").replace(0, np.nan)
        z = ((s[column] - median) / mad).where(groups.transform("count") >= MIN_POINTS_FOR_OUTLIERS)
        for _, row in s[z.abs() > 6].head(MAX_ROWS_PER_FLAG).iterrows():
            add(column, "OUTLIER", f"{label(row)}: {row[column]:.4g} (|z|>6)", row[year_col])

        ordered = s.sort_values(series_keys + [year_col])
        ordered_groups = ordered.groupby(series_keys, observed=True)[column]
        ratio = ordered_groups.pct_change().add(1).abs()
        with np.errstate(divide="ignore", invalid="ignore"):
            # A jump from or to zero has no ratio: it is not the ×10 signature of a unit error.
            jumps = np.log10(ratio.replace([0, np.inf], np.nan).astype(float)).abs() >= 1
        for _, row in ordered[jumps].head(MAX_ROWS_PER_FLAG).iterrows():
            add(column, "BREAK", f"{label(row)}: ≥×10 jump from the previous period", row[year_col])

        coverage = s.groupby(year_col)["country"].nunique()
        if len(coverage) > 1 and coverage.iloc[-1] < 0.7 * coverage.iloc[-2]:
            detail = f"{coverage.index[-1]}: {coverage.iloc[-1]} countries vs {coverage.iloc[-2]}"
            add(column, "COVERAGE", detail, coverage.index[-1])

        # Repeated zeroes are normal for sparse counts and do not count as a forward-fill.
        repeats = (ordered[column] == ordered_groups.shift()) & (ordered[column] != 0)
        share = repeats.groupby([ordered[k] for k in series_keys], observed=True).mean()
        for series in share[share > 0.5].index[:MAX_CONSTANT_SERIES]:
            name = ", ".join(map(str, series)) if isinstance(series, tuple) else str(series)
            add(column, "CONSTANT", f"{name}: >50% of series identical to previous period")

        if countries and _is_additive(unit) and not dims and (s["country"] == "World").any():
            latest = s.loc[s["country"] == "World", year_col].max()
            at_latest = s[s[year_col] == latest]
            world = at_latest.loc[at_latest["country"] == "World", column].iloc[0]
            total = at_latest.loc[at_latest["country"].isin(countries), column].sum()
            if world and abs(world - total) / abs(world) > 0.05:
                add(column, "WORLD_SUM", f"{latest}: World={world:.4g} vs sum of countries={total:.4g}", latest)
    return findings


def infer_dims(tb: pd.DataFrame, keys: list[str]) -> list[str]:
    """Extra dimensions of a table whose primary key is not known.

    A wide table has one row per entity and period: its text columns are categorical indicators, not dimensions.
    When rows repeat within an entity and period, the text columns with few distinct values are what tells them
    apart. A text column with many distinct values (a note, a source label) is never a dimension: keying on it
    would cut every series to one point.
    """
    if not keys or not tb.duplicated(keys).any():
        return []
    return [
        c
        for c in tb.columns
        if c not in keys
        and not _is_value(tb[c])
        and tb[c].nunique(dropna=True) <= min(MAX_DIMENSION_VALUES, len(tb) // 2)
    ]


def _is_value(column: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(column) and not pd.api.types.is_bool_dtype(column)


def _is_additive(unit: str) -> bool:
    return any(word in unit for word in ADDITIVE_WORDS) and not any(marker in unit for marker in RATE_MARKERS)


def country_names(data_dir: Path) -> set[str] | None:
    """Names of the entities that are countries (not continents or aggregates), or None when regions is not built."""
    from owid.catalog import Dataset

    versions = sorted((data_dir / "garden" / "regions").glob("*/regions"))
    if not versions:
        return None
    regions = Dataset(versions[-1]).read("regions")
    return set(regions.loc[regions["region_type"] == "country", "name"])


def scan_dataset(
    path: Path, columns: set[str] | None = None, entities: set[str] | None = None
) -> tuple[list[dict], list[str]]:
    """Flags for every table of a built dataset, and notes on what could not be judged."""
    from owid.catalog import Dataset

    dataset = Dataset(path)
    countries = country_names(path.parents[3])
    notes = [] if countries else ["WORLD_SUM not judged: the regions dataset is not built locally"]
    findings: list[dict] = []
    judged = 0
    matched: set[str] = set()
    for table_name in dataset.table_names:
        tb = dataset.read(table_name, safe_types=False)
        if tb.index.names != [None]:
            tb = tb.reset_index()
        units = {c: (tb[c].metadata.unit or "") for c in tb.columns}
        # The table's primary key says which columns are dimensions; without one they are inferred.
        meta_file = path / f"{table_name}.meta.json"
        primary_key = json.loads(meta_file.read_text()).get("primary_key") if meta_file.exists() else None
        dims = [k for k in primary_key if k not in ("country", "year", "date")] if primary_key else None
        frame = pd.DataFrame(tb)
        if entities and "country" in frame.columns:
            frame = frame[frame["country"].isin(entities)]
        if columns:
            wanted = {c for c in frame.columns if any(column_matches(name, c) for name in columns)}
            matched |= {name for name in columns if any(column_matches(name, c) for c in wanted)}
            keep = [
                c
                for c in frame.columns
                if c in wanted or c in ("country", "year", "date") or c in (dims or []) or not _is_value(frame[c])
            ]
            frame = frame[keep]
        judged += sum(1 for c in frame.columns if c not in ("year", "date") and _is_value(frame[c]))
        findings += scan_table(frame, table_name, units, countries, dims)
    notes.insert(0, f"{judged} numeric columns judged in {len(dataset.table_names)} table(s)")
    if columns and columns - matched:
        notes.append(
            "NOT FOUND in this dataset, so not judged: " + ", ".join(f"`{c}`" for c in sorted(columns - matched))
        )
    return findings, notes


def column_matches(requested: str, column: str) -> bool:
    """Whether a requested name is this garden column, also when given as a grapher name.

    A grapher column flattens the garden dimensions into its name (`libdem_vdem__estimate_best` is garden
    `libdem_vdem` at `estimate=best`), so a grapher name selects the garden column it starts with.
    """
    return requested == column or requested.startswith(f"{column}__")


def write_csv(findings: list[dict], out: Path) -> None:
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["table", "column", "kind", "period", "detail"])
        writer.writeheader()
        writer.writerows(findings)


def main() -> None:
    from etl import paths

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", help="<namespace>/<version>/<short_name> of the built garden dataset")
    parser.add_argument("--columns", help="comma-separated columns to scan (default: all)")
    parser.add_argument("--entities", help="comma-separated entities to scan (default: all)")
    parser.add_argument("--out", type=Path, help="write the flags to this CSV")
    args = parser.parse_args()
    namespace, version, short_name = args.step.replace("data://", "").strip("/").split("/")[-3:]
    path = paths.DATA_DIR / "garden" / namespace / version / short_name
    if not path.exists():
        sys.exit(f"Not built: {path}. Run `.venv/bin/etlr data://garden/{namespace}/{version}/{short_name}` first.")
    findings, notes = scan_dataset(
        path,
        set(args.columns.split(",")) if args.columns else None,
        set(args.entities.split(",")) if args.entities else None,
    )
    for note in notes:
        print(note)
    for finding in findings:
        print(f"{finding['kind']:9s} {finding['table']}.{finding['column']}: {finding['detail']}")
    if args.out:
        write_csv(findings, args.out)


if __name__ == "__main__":
    main()
