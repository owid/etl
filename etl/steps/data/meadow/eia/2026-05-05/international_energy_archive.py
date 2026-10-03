"""Read the historical EIA International Energy Statistics archive.

The snapshot is a zip with one folder per topic (``coal_consumption``, ``oil_reserves``, …),
each containing one ``INT-Export-…csv`` exported from EIA's old international tool. Annual
exports come in two layouts depending on how the user navigated the tool:

**Flat layout** (one indicator, many countries) — used for the reserves and dry-natural-gas files::

    Row 1: ``Report generated on: …``
    Row 2: ``"API","",<year_1>,<year_2>,…``
    Row 3: ``"","<indicator name> (<unit>)",,,…``
    Row 4+: ``"<api_id>","    <country>",<v1>,<v2>,…``

**Nested layout** (one country group with sub-indicators inside) — used for coal trade/production
and oil consumption/production. Indentation in the label column encodes hierarchy::

    Row 1: ``Report generated on: …``
    Row 2: ``"API","",<year_1>,<year_2>,…``
    Row 3+: ``"","World",,,…``                 (depth-0 entity; no API)
            ``"","    Production",,,…``        (depth-1 category; no API)
            ``"INTL.7-1-WORL-TST.A","        Coal (Mst)",<v1>,…``  (depth-2 data row)
            ``"INTL.11-1-WORL-TST.A","            Anthracite (Mst)",<v1>,…`` (depth-3 data row)
            ``"","Afghanistan",,,…``           (next country group)
            …

We detect the layout from the first non-header label and emit a single ``annual`` table keyed
by (topic, country, year, api). The ``api`` column preserves EIA's stable series identifier so
different sub-indicators that share a label stay distinct.

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


def _is_annual_period_header(period_columns: list[str]) -> bool:
    """Return True if the header row is annual (4-digit year columns), False otherwise."""
    sample = period_columns[0].strip()
    return sample.isdigit() and len(sample) == 4


def _parse_indicator_and_unit(label: str) -> tuple[str, str]:
    """Split a label like ``"natural gas reserves (tcf)"`` into (indicator, unit)."""
    m = LABEL_WITH_UNIT_RE.match(label.strip())
    if m:
        return m.group("indicator").strip(), m.group("unit").strip()
    return label.strip(), ""


def _read_flat_export(raw: pd.DataFrame, year_columns: list[str]) -> pd.DataFrame:
    """Parse a flat layout: one (indicator, unit) header in row 1; rows 2+ are countries with API."""
    indicator, unit = _parse_indicator_and_unit(str(raw.iloc[1, 1]))
    data = raw.iloc[2:].reset_index(drop=True).copy()
    data.columns = ["api", "country"] + year_columns
    data["country"] = data["country"].str.strip()
    data = data[data["country"].astype(bool)].reset_index(drop=True)
    data["indicator"] = indicator
    data["unit"] = unit
    return data


def _read_nested_export(raw: pd.DataFrame, year_columns: list[str]) -> pd.DataFrame:
    """Parse a nested layout: hierarchy is encoded in label indentation; data rows are leaves with API.

    Walk the rows; keep the most recent depth-0 label as the country; for each row that has an
    API code, take its own (stripped) label as the indicator+unit string.
    """
    body = raw.iloc[1:].reset_index(drop=True)
    body.columns = ["api", "label"] + year_columns

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
        for col in year_columns:
            out[col] = r[col]
        rows.append(out)
    return pd.DataFrame(rows)


def _read_annual_export_csv(content: bytes) -> pd.DataFrame | None:
    """Parse one annual EIA export CSV → long-format DataFrame with columns
    country, year, api, indicator, unit, value. Returns None for non-annual files."""
    raw = pd.read_csv(io.BytesIO(content), header=None, dtype=str, skiprows=PREAMBLE_ROWS, encoding="utf-8-sig")

    # Header row (after the preamble) is "API", "", year_1, year_2, …
    period_columns = raw.iloc[0, 2:].tolist()
    if not _is_annual_period_header(period_columns):
        # Skip monthly / quarterly exports — we only ingest annual data here.
        return None
    years = [int(c) for c in period_columns]

    # Detect layout by looking at the first label after the header. A parenthesised unit means
    # flat; a plain entity name means nested.
    label_after_header = str(raw.iloc[1, 1]) if len(raw) > 1 else ""
    if LABEL_WITH_UNIT_RE.match(label_after_header.strip()):
        wide = _read_flat_export(raw, period_columns)
    else:
        wide = _read_nested_export(raw, period_columns)

    # Melt to long.
    long = wide.melt(
        id_vars=["api", "country", "indicator", "unit"],
        value_vars=period_columns,
        var_name="_year_str",
        value_name="value",
    )
    long["year"] = long["_year_str"].map(dict(zip(period_columns, years)))
    long = long.drop(columns=["_year_str"])

    # Sentinels used by EIA: "--", "NA", empty, "w" (withheld).
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long.dropna(subset=["value"]).reset_index(drop=True)


def read_archive_zip(snap: Snapshot) -> Table:
    """Walk every annual CSV in the archive zip and build a single long-format table."""
    rows: list[pd.DataFrame] = []

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
                df = _read_annual_export_csv(content)
            except Exception as exc:  # noqa: BLE001
                paths.log.warning(f"Skipping {member!r}: failed to parse as EIA export ({exc})")
                continue
            if df is None:
                # Non-annual file (e.g. the monthly oil production export) — skip it.
                continue
            # Topic = the parent folder name (e.g. "coal_production", "oil_consumption"). This
            # disambiguates rows where the same indicator label (e.g. "Coal" with unit "Mst")
            # appears in multiple files for different metrics (production vs consumption etc.).
            parts = member.split("/")
            topic = parts[-2] if len(parts) >= 2 else ""
            df.insert(0, "topic", topic)
            rows.append(df)

    if not rows:
        raise RuntimeError("No annual EIA export CSVs found in archive zip.")

    df = pd.concat(rows, ignore_index=True)
    # The zip occasionally contains two CSVs for the same topic — same data exported at
    # different timestamps. Drop those duplicates (identical (topic, country, year, api) rows).
    df = df.drop_duplicates(subset=["topic", "country", "year", "api"], keep="last").reset_index(drop=True)
    # Categoricals to keep the feather small.
    for col in ["topic", "country", "indicator", "unit", "api"]:
        df[col] = df[col].astype("category")

    tb = Table(df[["topic", "country", "year", "api", "indicator", "unit", "value"]])
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("international_energy_archive.zip")

    #
    # Process data.
    #
    tb = read_archive_zip(snap).format(keys=["topic", "country", "year", "api"], short_name="annual")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
