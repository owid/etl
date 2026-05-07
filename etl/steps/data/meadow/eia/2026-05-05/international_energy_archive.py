"""Read the historical EIA International Energy Statistics archive.

The snapshot is a zip with one folder per topic (``coal_consumption``, ``oil_reserves``, …),
each containing one ``INT-Export-…csv`` exported from EIA's old international tool. The exports
come in two layouts depending on how the user navigated the tool:

**Flat layout** (one indicator, many countries) — used for the reserves and dry-natural-gas files::

    Row 1: ``Report generated on: …``
    Row 2: ``"API","",<period_1>,<period_2>,…``
    Row 3: ``"","<indicator name> (<unit>)",,,…``
    Row 4+: ``"<api_id>","    <country>",<v1>,<v2>,…``

**Nested layout** (one country group with sub-indicators inside) — used for coal trade/production
and oil consumption/production. Indentation in the label column encodes hierarchy::

    Row 1: ``Report generated on: …``
    Row 2: ``"API","",<period_1>,<period_2>,…``
    Row 3+: ``"","World",,,…``                 (depth-0 entity; no API)
            ``"","    Production",,,…``        (depth-1 category; no API)
            ``"INTL.7-1-WORL-TST.A","        Coal (Mst)",<v1>,…``  (depth-2 data row)
            ``"INTL.11-1-WORL-TST.A","            Anthracite (Mst)",<v1>,…`` (depth-3 data row)
            ``"","Afghanistan",,,…``           (next country group)
            …

We detect the layout from the first non-header label and emit two tables — ``annual`` and
``monthly`` — keyed by (country, period, api). The ``api`` column preserves EIA's stable
series identifier so different sub-indicators that share a label stay distinct.
"""

import io
import re
import zipfile

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# Number of free-text rows at the top of every EIA web-tool export before the proper header.
PREAMBLE_ROWS = 1
# Pattern that matches a label ending with a parenthesised unit, e.g. "natural gas reserves (tcf)".
LABEL_WITH_UNIT_RE = re.compile(r"^(?P<indicator>.+?)\s*\((?P<unit>[^()]+)\)\s*$")


def _parse_indicator_and_unit(label: str) -> tuple[str, str]:
    """Split a label like ``"natural gas reserves (tcf)"`` into (indicator, unit)."""
    m = LABEL_WITH_UNIT_RE.match(label.strip())
    if m:
        return m.group("indicator").strip(), m.group("unit").strip()
    return label.strip(), ""


def _parse_period_columns(period_columns: list[str]) -> tuple[str, list]:
    """Detect frequency from the period header. Returns ('annual', [int]) or ('monthly', [pd.Timestamp])."""
    sample = period_columns[0].strip()
    if sample.isdigit() and len(sample) == 4:
        return "annual", [int(c) for c in period_columns]
    return "monthly", pd.to_datetime(period_columns, format="%b %Y").tolist()


def _read_flat_export(raw: pd.DataFrame, period_columns: list[str], periods: list) -> pd.DataFrame:
    """Parse a flat layout: one (indicator, unit) header in row 1; rows 2+ are countries with API."""
    indicator, unit = _parse_indicator_and_unit(str(raw.iloc[1, 1]))
    data = raw.iloc[2:].reset_index(drop=True).copy()
    data.columns = ["api", "country"] + period_columns
    data["country"] = data["country"].str.strip()
    data = data[data["country"].astype(bool)].reset_index(drop=True)
    data["indicator"] = indicator
    data["unit"] = unit
    return data


def _read_nested_export(raw: pd.DataFrame, period_columns: list[str], periods: list) -> pd.DataFrame:
    """Parse a nested layout: hierarchy is encoded in label indentation; data rows are leaves with API.

    Walk the rows; keep the most recent depth-0 label as the country; for each row that has an
    API code, take its own (stripped) label as the indicator+unit string.
    """
    body = raw.iloc[1:].reset_index(drop=True)
    body.columns = ["api", "label"] + period_columns

    rows = []
    current_country = None
    for _, r in body.iterrows():
        label_raw = "" if pd.isna(r["label"]) else str(r["label"])
        if not label_raw.strip():
            continue
        depth = len(label_raw) - len(label_raw.lstrip(" "))
        api = "" if pd.isna(r["api"]) else str(r["api"]).strip()
        if not api:
            # Header row. We only care about depth-0 labels (country / region names).
            if depth == 0:
                current_country = label_raw.strip()
            continue
        # Data row.
        if current_country is None:
            continue
        indicator, unit = _parse_indicator_and_unit(label_raw)
        out = {"api": api, "country": current_country, "indicator": indicator, "unit": unit}
        for col in period_columns:
            out[col] = r[col]
        rows.append(out)
    return pd.DataFrame(rows)


def _read_eia_export_csv(content: bytes) -> tuple[str, pd.DataFrame]:
    """Parse one EIA export CSV → (frequency, long-format DataFrame with columns
    country, year_or_date, api, indicator, unit, value).
    """
    raw = pd.read_csv(io.BytesIO(content), header=None, dtype=str, skiprows=PREAMBLE_ROWS, encoding="utf-8-sig")

    # Header row (after the preamble) is "API", "", period_1, period_2, …
    period_columns = raw.iloc[0, 2:].tolist()
    frequency, periods = _parse_period_columns(period_columns)

    # Detect layout by looking at the first label after the header. A parenthesised unit means
    # flat; a plain entity name means nested.
    label_after_header = str(raw.iloc[1, 1]) if len(raw) > 1 else ""
    if LABEL_WITH_UNIT_RE.match(label_after_header.strip()):
        wide = _read_flat_export(raw, period_columns, periods)
    else:
        wide = _read_nested_export(raw, period_columns, periods)

    # Melt to long.
    long = wide.melt(
        id_vars=["api", "country", "indicator", "unit"],
        value_vars=period_columns,
        var_name="_period",
        value_name="value",
    )
    long["_period_parsed"] = long["_period"].map(dict(zip(period_columns, periods)))
    long = long.drop(columns=["_period"])
    long = long.rename(columns={"_period_parsed": "year" if frequency == "annual" else "date"})

    # Sentinels used by EIA: "--", "NA", empty, "w" (withheld).
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"]).reset_index(drop=True)
    return frequency, long


def read_archive_zip(snap: Snapshot) -> tuple[Table, Table]:
    """Walk every CSV in the archive zip and build (annual_table, monthly_table)."""
    annual_rows: list[pd.DataFrame] = []
    monthly_rows: list[pd.DataFrame] = []

    with zipfile.ZipFile(snap.path, "r") as zf:
        for member in sorted(zf.namelist()):
            if not member.lower().endswith(".csv"):
                continue
            # Skip macOS resource-fork files (_*.csv inside __MACOSX/) that ship in zips made on a Mac.
            if "__MACOSX" in member or "/._" in member:
                continue
            with zf.open(member) as fh:
                content = fh.read()
            try:
                frequency, df = _read_eia_export_csv(content)
            except Exception as exc:  # noqa: BLE001
                # Skip files that don't match the expected EIA export shape, but be loud.
                paths.log.warning(f"Skipping {member!r}: failed to parse as EIA export ({exc})")
                continue
            # Topic = the parent folder name (e.g. "coal_production", "oil_consumption"). This
            # disambiguates rows where the same indicator label (e.g. "Coal" with unit "Mst")
            # appears in multiple files for different metrics (production vs consumption etc.).
            parts = member.split("/")
            topic = parts[-2] if len(parts) >= 2 else ""
            df.insert(0, "topic", topic)
            (annual_rows if frequency == "annual" else monthly_rows).append(df)

    if not annual_rows and not monthly_rows:
        raise RuntimeError("No EIA export CSVs found in archive zip.")

    def _consolidate(rows: list[pd.DataFrame], time_col: str) -> Table:
        df = pd.concat(rows, ignore_index=True)
        # The zip occasionally contains two CSVs for the same topic — same data exported at
        # different timestamps. Drop those duplicates (after concat, identical (topic, country,
        # period, api) rows are by definition redundant).
        df = df.drop_duplicates(subset=["topic", "country", time_col, "api"], keep="last").reset_index(drop=True)
        # Categoricals to keep the feather small.
        for col in ["topic", "country", "indicator", "unit", "api"]:
            df[col] = df[col].astype("category")
        return Table(df[["topic", "country", time_col, "api", "indicator", "unit", "value"]])

    tb_annual = _consolidate(annual_rows, "year") if annual_rows else None
    tb_monthly = _consolidate(monthly_rows, "date") if monthly_rows else None

    # Restore origins on derived columns.
    for tb in (tb_annual, tb_monthly):
        if tb is None:
            continue
        for col in tb.columns:
            tb[col].metadata.origins = [snap.metadata.origin]

    return tb_annual, tb_monthly


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("international_energy_archive.zip")

    #
    # Process data.
    #
    tb_annual, tb_monthly = read_archive_zip(snap)
    tables = []
    if tb_annual is not None:
        tables.append(tb_annual.format(keys=["topic", "country", "year", "api"], short_name="annual"))
    if tb_monthly is not None:
        tables.append(tb_monthly.format(keys=["topic", "country", "date", "api"], short_name="monthly"))

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables)
    ds_meadow.save()
