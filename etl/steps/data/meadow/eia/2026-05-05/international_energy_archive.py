"""Read the historical EIA International Energy Statistics archive.

The snapshot is a zip with one folder per indicator (``coal_consumption``, ``oil_reserves``, …),
each containing a single ``INT-Export-…csv`` file in EIA's web-tool export format:

    Row 1: ``Report generated on: …`` (free-text header)
    Row 2: ``"API","",<period_1>,<period_2>,…``  (period = year for annual, "Mon YYYY" for monthly)
    Row 3: ``"","<indicator name> (<unit>)",,,…``
    Row 4+: ``"<api_id>","   <country>",<v1>,<v2>,…``

We reshape every CSV into long format and emit two tables — ``annual`` (year integer) and
``monthly`` (date) — keyed by (country, period, api). The ``api`` column preserves EIA's
series identifier so ambiguous indicators stay distinct.
"""

import io
import zipfile

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# Number of free-text rows at the top of every EIA web-tool export before the proper header.
PREAMBLE_ROWS = 1


def _parse_indicator_and_unit(label: str) -> tuple[str, str]:
    """Split an EIA label like ``"natural gas reserves (tcf)"`` into (indicator, unit)."""
    label = label.strip()
    if label.endswith(")") and "(" in label:
        idx = label.rfind("(")
        return label[:idx].strip(), label[idx + 1 : -1].strip()
    return label, ""


def _parse_period_columns(period_columns: list[str]) -> tuple[str, list]:
    """Detect frequency from the period header and parse it.

    Returns ('annual', [int]) or ('monthly', [pd.Timestamp]).
    """
    sample = period_columns[0].strip()
    if sample.isdigit() and len(sample) == 4:
        return "annual", [int(c) for c in period_columns]
    return "monthly", pd.to_datetime(period_columns, format="%b %Y").tolist()


def _read_eia_export_csv(content: bytes) -> tuple[str, pd.DataFrame]:
    """Parse one ``INT-Export-*.csv`` file → (frequency, long-format DataFrame).

    The returned DataFrame has columns: country, year_or_date, api, indicator, unit, value.
    """
    raw = pd.read_csv(io.BytesIO(content), header=None, dtype=str, skiprows=PREAMBLE_ROWS, encoding="utf-8-sig")

    # Row 0 is the header: "API", "", year_1, year_2, …
    period_columns = raw.iloc[0, 2:].tolist()
    frequency, periods = _parse_period_columns(period_columns)

    # Row 1 is the indicator + unit label in the second cell.
    indicator, unit = _parse_indicator_and_unit(str(raw.iloc[1, 1]))

    # Row 2 onwards is data.
    data = raw.iloc[2:].reset_index(drop=True)
    data.columns = ["api", "country"] + period_columns
    data["country"] = data["country"].str.strip()
    # Drop blank rows (some exports trail empty rows).
    data = data[data["country"].astype(bool)].reset_index(drop=True)

    # Melt to long. ``var_name`` placeholder is replaced after melt because the period column was
    # parsed once and we want the parsed (int year or datetime) version.
    long = data.melt(id_vars=["api", "country"], value_vars=period_columns, var_name="_period", value_name="value")
    long["_period_parsed"] = long["_period"].map(dict(zip(period_columns, periods)))
    long = long.drop(columns=["_period"])
    long = long.rename(columns={"_period_parsed": "year" if frequency == "annual" else "date"})
    long["indicator"] = indicator
    long["unit"] = unit

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
            with zf.open(member) as fh:
                content = fh.read()
            try:
                frequency, df = _read_eia_export_csv(content)
            except Exception as exc:  # noqa: BLE001
                # Skip files that don't match the expected EIA export shape, but be loud.
                paths.log.warning(f"Skipping {member!r}: failed to parse as EIA export ({exc})")
                continue
            (annual_rows if frequency == "annual" else monthly_rows).append(df)

    if not annual_rows and not monthly_rows:
        raise RuntimeError("No EIA export CSVs found in archive zip.")

    def _consolidate(rows: list[pd.DataFrame], time_col: str) -> Table:
        df = pd.concat(rows, ignore_index=True)
        # Categoricals to keep the feather small.
        for col in ["country", "indicator", "unit", "api"]:
            df[col] = df[col].astype("category")
        return Table(df[["country", time_col, "api", "indicator", "unit", "value"]])

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
        tables.append(
            tb_annual.format(keys=["country", "year", "api"], short_name="annual")
        )
    if tb_monthly is not None:
        tables.append(
            tb_monthly.format(keys=["country", "date", "api"], short_name="monthly")
        )

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables)
    ds_meadow.save()
