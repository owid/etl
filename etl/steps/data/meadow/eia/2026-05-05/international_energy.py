"""Load a snapshot and create a meadow dataset.

The EIA International Energy bulk file packs every (variable, country, frequency) timeseries
into one row, with the data as a list of [year, value] pairs. This step reshapes the raw JSON
into a long-format table indexed by (country, year, variable, unit) so the garden step can
filter and pivot whichever indicators it needs.
"""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# We keep only annual series; the raw file also contains monthly ("M") and quarterly ("Q").
ANNUAL_FREQUENCY_CODE = "A"
ANNUAL_NAME_SUFFIX = ", Annual"


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


def reshape_eia_data(data_raw: Table, snap: Snapshot) -> Table:
    """Reshape the raw EIA bulk JSON into a long-format table.

    The `name` field encodes ``"{variable}, {country}, Annual"`` — but both the variable
    (``"Crude oil, NGPL, and other liquids production"``) and the country (``"Germany, East"``)
    can contain commas, so a comma split is not safe. Instead we use ``geoset_id``, which groups
    all rows for a single (variable, unit) across countries: the longest common prefix of names
    in a group is ``"{variable}, "``.
    """
    # Drop rows without a data payload and keep only annual series.
    data = data_raw.dropna(subset=["data"])
    data = data[data["f"] == ANNUAL_FREQUENCY_CODE].reset_index(drop=True)

    # Recover variable + country from `name` using the LCP-per-geoset_id trick described above.
    name_stripped = data["name"].str[: -len(ANNUAL_NAME_SUFFIX)]
    variable_by_geoset = {
        gid: _longest_common_prefix(group.unique().tolist()).rsplit(", ", 1)[0]
        for gid, group in name_stripped.groupby(data["geoset_id"], observed=True)
    }
    data["variable"] = data["geoset_id"].map(variable_by_geoset)
    data["country"] = [n[len(v) + 2 :] for n, v in zip(name_stripped, data["variable"])]

    # The raw bulk file has occasional duplicate (country, variable, unit) entries; keep one.
    data = data.drop_duplicates(subset=["country", "variable", "units"], keep="last")

    # Expand list-of-[year, value] into one row per year and rename columns to OWID conventions.
    data = data[["country", "variable", "units", "data", "geography"]].rename(
        columns={"units": "unit", "data": "values", "geography": "members"}
    )
    data = data.explode("values", ignore_index=True)
    data["year"] = data["values"].str[0]
    data["value"] = data["values"].str[1]
    data = data.drop(columns=["values"])

    # Sentinels in the raw data: "--" (no data), "NA" (not available), "w" (withheld).
    data["value"] = pd.to_numeric(data["value"], errors="coerce")

    # A handful of series end with an empty entry; drop those, then year is safe to cast.
    data = data.dropna(subset=["year"]).astype({"year": int}).reset_index(drop=True)

    # `value` was unpacked from list elements, so it lost the origins from `snap.read_json`;
    # the index columns keep theirs and don't need restoration.
    data["value"].metadata.origins = [snap.metadata.origin]

    # Low-cardinality strings: storing them as categoricals shrinks the feather ~4×.
    for col in ["country", "variable", "unit", "members"]:
        data[col] = data[col].astype("category")

    return data.format(keys=["country", "year", "variable", "unit"], short_name=paths.short_name)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("international_energy.zip")
    data_raw = snap.read_json(lines=True)

    #
    # Process data.
    #
    tb = reshape_eia_data(data_raw=data_raw, snap=snap)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
