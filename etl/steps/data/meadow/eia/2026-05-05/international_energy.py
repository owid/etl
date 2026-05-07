"""Load a snapshot and create a meadow dataset.

The EIA International Energy bulk file packs every (variable, country, frequency) timeseries
into one row, with the data as a list of [period, value] pairs. This step reshapes the raw JSON
into long-format tables indexed by (country, period, variable, unit) so the garden step can
filter and pivot whichever indicators it needs.

Two tables are emitted:

- ``international_energy`` — annual series only (the bulk of the data).
- ``international_energy_monthly`` — monthly series only (mainly oil & petroleum production).

Quarterly series are dropped: they're a small subset of what's in monthly form already.
"""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# Frequency codes in the raw data and the name suffix EIA appends to each row's name field.
FREQUENCIES = {
    "A": ("Annual", "year"),
    "M": ("Monthly", "date"),
}


def _longest_common_prefix(strings: list[str]) -> str:
    """Return the longest string that is a prefix of every input string."""
    if not strings:
        return ""
    shortest = min(strings)
    longest = max(strings)
    i = 0
    while i < len(shortest) and i < len(longest) and shortest[i] == longest[i]:
        i += 1
    return shortest[:i]


def reshape_eia_data(data_raw: Table, snap: Snapshot, frequency: str) -> Table:
    """Reshape the raw EIA bulk JSON, restricted to the given frequency, into a long-format table.

    The `name` field encodes ``"{variable}, {country}, {Annual|Monthly|...}"`` — but both the
    variable (``"Crude oil, NGPL, and other liquids production"``) and the country
    (``"Germany, East"``) can contain commas, so a comma split is not safe. We instead use
    ``geoset_id`` (a (variable, unit, frequency) identifier shared across countries): the longest
    common prefix of names within a group is ``"{variable}, "``.
    """
    freq_label, time_col = FREQUENCIES[frequency]
    name_suffix = f", {freq_label}"

    # Drop rows without a data payload and keep only the requested frequency.
    data = data_raw.dropna(subset=["data"])
    data = data[data["f"] == frequency].reset_index(drop=True)

    # Recover variable + country from `name` using the LCP-per-geoset_id trick described above.
    name_stripped = data["name"].str[: -len(name_suffix)]
    variable_by_geoset = {
        gid: _longest_common_prefix(group.unique().tolist()).rsplit(", ", 1)[0]
        for gid, group in name_stripped.groupby(data["geoset_id"], observed=True)
    }
    data["variable"] = data["geoset_id"].map(variable_by_geoset)
    data["country"] = [n[len(v) + 2 :] for n, v in zip(name_stripped, data["variable"])]

    # The raw bulk file has occasional duplicate (country, variable, unit) entries; keep one.
    data = data.drop_duplicates(subset=["country", "variable", "units"], keep="last")

    # Expand list-of-[period, value] into one row per period and rename columns to OWID conventions.
    data = data[["country", "variable", "units", "data", "geography"]].rename(
        columns={"units": "unit", "data": "values", "geography": "members"}
    )
    data = data.explode("values", ignore_index=True)
    data["period"] = data["values"].str[0]
    data["value"] = data["values"].str[1]
    data = data.drop(columns=["values"])

    # Sentinels in the raw data: "--" (no data), "NA" (not available), "w" (withheld).
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data = data.dropna(subset=["period"]).reset_index(drop=True)

    # Parse the period column according to frequency.
    if frequency == "A":
        data[time_col] = data["period"].astype(int)
    else:  # Monthly: YYYYMM string → first-of-month date.
        data[time_col] = pd.to_datetime(data["period"].astype(str), format="%Y%m")
    data = data.drop(columns=["period"])

    # `value` was unpacked from list elements, so it lost the origins from `snap.read_json`;
    # the index columns keep theirs and don't need restoration.
    data["value"].metadata.origins = [snap.metadata.origin]

    # Low-cardinality strings: storing them as categoricals shrinks the feather ~4×.
    for col in ["country", "variable", "unit", "members"]:
        data[col] = data[col].astype("category")

    short_name = paths.short_name if frequency == "A" else f"{paths.short_name}_monthly"
    return data.format(keys=["country", time_col, "variable", "unit"], short_name=short_name)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("international_energy.zip")
    data_raw = snap.read_json(lines=True)

    #
    # Process data.
    #
    tb_annual = reshape_eia_data(data_raw=data_raw, snap=snap, frequency="A")
    tb_monthly = reshape_eia_data(data_raw=data_raw, snap=snap, frequency="M")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb_annual, tb_monthly])
    ds_meadow.save()
