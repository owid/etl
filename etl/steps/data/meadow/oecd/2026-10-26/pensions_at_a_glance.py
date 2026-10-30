"""Load a snapshot and create a meadow dataset."""

import re

from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep from the SDMX file, and their new names. Dimensions are kept as codes, which are more stable than labels.
COLUMNS = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "MEASURE": "measure",
    "UNIT_MEASURE": "unit_measure",
    "SEX": "sex",
    "AGE": "age",
    "OPTIONALITY": "optionality",
    "OBS_VALUE": "value",
}

# Year of the tax rules behind the tax rates of Figure 4.3, which the spreadsheet doesn't state.
# NOTE: Update it with the new edition of the report.
TAX_RATES_YEAR = 2024

# Year in which the modeled worker of Figure 4.2 starts working, which the spreadsheet doesn't state.
# NOTE: Update it with the new edition of the report.
REPLACEMENT_RATES_AT_AGE_80_YEAR = 2024


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = paths.load_snapshot("pensions_at_a_glance.csv")
    snap_taxes = paths.load_snapshot("pensions_at_a_glance_tax_rates.xlsx")
    snap_age_80 = paths.load_snapshot("pensions_at_a_glance_replacement_rates_at_age_80.xlsx")

    # Load data from snapshots.
    tb = snap.read()
    tb_taxes = read_tax_rates(snap=snap_taxes)
    tb_age_80 = read_replacement_rates_at_age_80(snap=snap_age_80)

    #
    # Process data.
    #
    # Check that the columns we drop carry no information.
    for column, expected in {"OBS_STATUS": "A", "UNIT_MULT": 0, "FREQ": "A"}.items():
        assert set(tb[column].unique()) == {expected}, f"Unexpected values in {column}: {set(tb[column].unique())}"

    # Keep and rename relevant columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Use categoricals for low-cardinality columns, and improve table format.
    for column in ["country", "measure", "unit_measure", "sex", "age", "optionality"]:
        tb[column] = tb[column].astype("category")
    tables = [
        tb.format(["country", "year", "measure", "unit_measure", "sex", "age", "optionality"]),
        tb_taxes.format(["country", "year"], short_name="tax_rates"),
        tb_age_80.format(["country", "year"], short_name="replacement_rates_at_age_80"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def read_tax_rates(snap: Snapshot) -> Table:
    """Read the tax rates of workers and pensioners from the data sheet of Figure 4.3.

    The sheet has two blocks side by side: the stacked bars of the figure (in percent) and, to their right, the rates
    themselves (as fractions). Each block starts with a header row containing "Earnings"; we read the second block.
    """
    tb_raw = snap.read_excel(sheet_name="Data", header=None)
    header_rows = [i for i in range(len(tb_raw)) if (tb_raw.iloc[i] == "Earnings").sum() == 2]
    assert len(header_rows) == 1, "Unexpected layout of the Figure 4.3 data sheet."
    header_row = header_rows[0]
    column = [c for c in tb_raw.columns if tb_raw.iloc[header_row][c] == "Earnings"][1]
    header = list(tb_raw.iloc[header_row].loc[column : column + 3])
    assert header == ["Earnings", "tax_w", "tax_p", "tax_p2"], f"Unexpected columns in Figure 4.3 data: {header}"

    rows = []
    for _, row in tb_raw.iloc[header_row + 1 :].iterrows():
        country = row[column + 4]
        if isinstance(country, str):
            rows.append(
                {
                    # Asterisks mark countries where pensioners pay no taxes or contributions.
                    "country": country.rstrip("*").strip(),
                    "year": TAX_RATES_YEAR,
                    "tax_rate_workers": row[column + 1],
                    "tax_rate_pensioners_average_earner": row[column + 2],
                    "tax_rate_pensioners_income_equal_to_average_wage": row[column + 3],
                }
            )
    tb = snap.read_from_records(rows)
    tb = tb.astype({column: float for column in tb.columns if column.startswith("tax_rate")})

    return tb


def read_replacement_rates_at_age_80(snap: Snapshot) -> Table:
    """Read the gross replacement rates at retirement and at age 80 from the data sheet of Figure 4.2.

    Countries are labeled by their ISO code followed by their normal retirement age, e.g. "DNK (74)"; the OECD and EU
    averages have no age.
    """
    tb_raw = snap.read_excel(sheet_name="Data", header=None)
    header_rows = [i for i in range(len(tb_raw)) if tb_raw.iloc[i, 0] == "Country"]
    assert len(header_rows) == 1, "Unexpected layout of the Figure 4.2 data sheet."
    header_row = header_rows[0]
    header = list(tb_raw.iloc[header_row, :3])
    assert header == ["Country", "GRR at retirement age", "GRR at age 80"], (
        f"Unexpected columns in Figure 4.2: {header}"
    )

    rows = []
    for _, row in tb_raw.iloc[header_row + 1 :].iterrows():
        label = row[0]
        if isinstance(label, str):
            match = re.fullmatch(r"([A-Z0-9]+)(?: \((\d+)\))?", label.strip())
            assert match, f"Unexpected country label in Figure 4.2: {label}"
            rows.append(
                {
                    "country": match.group(1),
                    "year": REPLACEMENT_RATES_AT_AGE_80_YEAR,
                    "normal_retirement_age": float(match.group(2)) if match.group(2) else None,
                    "gross_replacement_rate_at_retirement": row[1],
                    "gross_replacement_rate_at_age_80": row[2],
                }
            )
    tb = snap.read_from_records(rows)
    tb = tb.astype({column: float for column in tb.columns if column not in ["country", "year"]})

    return tb
