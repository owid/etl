"""Load the UN's international migrant stock by age and sex, with the resident population it reports alongside it.

The workbook holds eight tables; the two read here are the counts the rest of it is derived from.
Both have the same layout -- one row per entity and year, and three blocks of the same sixteen age
bands, for both sexes, males and females -- so they are read the same way, and differ only in the
column they end up in and in Table 2 being published in thousands.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# The five-year bands the UN reports over, the oldest open-ended.
AGE_GROUPS = [
    "0-4",
    "5-9",
    "10-14",
    "15-19",
    "20-24",
    "25-29",
    "30-34",
    "35-39",
    "40-44",
    "45-49",
    "50-54",
    "55-59",
    "60-64",
    "65-69",
    "70-74",
    "75+",
]

# Each sheet repeats the age bands once per sex block -- both sexes, then males, then females --
# and pandas tells the repeats apart by suffixing them in the order they appear.
SEX_COLUMN_SUFFIX = {"all": "", "male": ".1", "female": ".2"}

# The row carrying the column names. Above it sits the sheet's title block.
HEADER_ROW = 10

# The sheets read, the column each becomes, and the factor turning its unit into people.
SHEETS = {
    "Table 1": ("migrant_stock", 1),
    "Table 2": ("population", 1000),
}

# Table 2 is published in thousands to three decimals, so scaling it lands a whole number of people
# up to the error of a binary float. Anything larger means the sheet is no longer what this step
# takes it for, rather than something to round away.
MAX_ROUNDING_ERROR = 1e-3


def read_sheet(snap: Snapshot, sheet: str, column: str, scale: int) -> Table:
    """Read one sheet into `country, year, location_code, sex, age, <column>`."""
    tb = snap.read_excel(sheet_name=sheet, header=HEADER_ROW, na_values=[".."])
    # Band columns carry stray whitespace (`" 15-19"`), which the sex suffix is appended after.
    tb = tb.rename(columns={name: str(name).strip() for name in tb.columns})
    tb = tb.rename(
        columns={
            "Region, development group, country or area": "country",
            "Year": "year",
            "Location code": "location_code",
        }
    )
    # Entity names are indented to show the hierarchy, and carry a trailing asterisk where the sheet
    # footnotes them -- neither is part of the name. `.str` returns a plain series, so the column's
    # metadata is put back on it.
    tb["country"] = tb["country"].str.strip().str.rstrip("*").str.strip().copy_metadata(tb["country"])

    tables = []
    for sex, suffix in SEX_COLUMN_SUFFIX.items():
        columns = {f"{age}{suffix}": age for age in AGE_GROUPS}
        # `Australia and New Zealand` is listed twice with the same values both times; only the row
        # number, which is not read here, tells the copies apart.
        table = tb[["country", "year", "location_code"] + list(columns)].rename(columns=columns).drop_duplicates()
        table = pr.melt(table, id_vars=["country", "year", "location_code"], var_name="age", value_name=column)
        table["sex"] = sex
        tables.append(table)

    tb = pr.concat(tables, ignore_index=True)
    # `..` marks a figure the UN does not report, and `na_values` has already made it missing;
    # anything else non-numeric would be a sheet this step has not seen.
    tb[column] = pr.to_numeric(tb[column]) * scale
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("migrant_stock_age_sex.xlsx")
    tables = [read_sheet(snap, sheet, column, scale) for sheet, (column, scale) in SHEETS.items()]

    #
    # Process data.
    #
    # Both sheets index the same entities, years and bands, so an outer join that validates one to
    # one is the check that they still do.
    keys = ["country", "year", "location_code", "sex", "age"]
    tb = pr.multi_merge(tables, on=keys, how="outer", validate="one_to_one")

    error = (tb["population"] - tb["population"].round()).abs().max()
    assert error < MAX_ROUNDING_ERROR, f"Population in thousands scales to {error} off a whole person, not ~0."
    tb["population"] = tb["population"].round()

    tb = tb.format(keys, short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
