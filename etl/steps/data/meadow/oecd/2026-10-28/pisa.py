"""Load PISA snapshots and create a meadow dataset.

The data comes from two sources:
- The annex tables of the PISA 2025 Results (Volume I) report (the Excel files behind its StatLinks): mean scores of all
  students for every PISA cycle (2000 to 2025), and of girls and boys from 2015 to 2025. They only cover the countries
  and economies that took part in PISA 2025.
- The PISA Data Explorer exports of the previous version (up to PISA 2022), which garden uses to fill in what the
  report tables don't cover (earlier participants, and girls and boys before 2015).

Both tables use the same column names as the previous version of this dataset.
"""

import re
from functools import reduce

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Annex tables with the mean score of all students, by PISA cycle.
TABLES_ALL = {
    "science": "Table I.B1.2a.36",
    "reading": "Table I.B1.2a.37",
    "math": "Table I.B1.2a.38",
}
# Annex tables with the mean score of girls and boys, by PISA cycle.
TABLES_BY_GENDER = {
    ("science", "girls"): "Table I.B1.2c.23",
    ("science", "boys"): "Table I.B1.2c.24",
    ("reading", "girls"): "Table I.B1.2c.26",
    ("reading", "boys"): "Table I.B1.2c.27",
    ("math", "girls"): "Table I.B1.2c.29",
    ("math", "boys"): "Table I.B1.2c.30",
}
# Values the report uses for missing data ("m": not available; "c": too few observations).
MISSING_MARKERS = {"m", "c"}
# The report marks countries that didn't meet one or more PISA sampling standards in 2025 with a trailing "*".
FLAG_SAMPLING_STANDARDS = "*"
# Countries whose results under "PISA 2018" are from the PISA for Development assessment, held in 2017 (per the note
# under each trend table). Garden moves them to 2017.
PISA_FOR_DEVELOPMENT_NOTE = re.compile(
    r"For (.+?), results reported under PISA 2018 refer to the results of the PISA for Development assessment, in 2017"
)
EXPECTED_PISA_FOR_DEVELOPMENT = {"Cambodia", "Ecuador", "Guatemala", "Paraguay", "Zambia"}


def run() -> None:
    #
    # Load inputs.
    #
    snap_all = paths.load_snapshot("pisa_mean_performance.xlsx")
    snap_gender = paths.load_snapshot("pisa_performance_by_gender.xlsx")

    #
    # Process data.
    #
    # Step 1: Read the mean scores of all students, and of girls and boys, from the PISA 2025 report tables.
    tbs = [read_cycle_table(snap_all, sheet, column=f"pisa_{subject}_all") for subject, sheet in TABLES_ALL.items()]
    tbs += [
        read_cycle_table(snap_gender, sheet, column=f"pisa_{subject}_boys_girls", suffix=f"_{group}")
        for (subject, group), sheet in TABLES_BY_GENDER.items()
    ]
    tb = reduce(lambda left, right: pr.merge(left, right, on=["country", "year"], how="outer"), tbs)

    # Step 2: Keep the report's sampling-standards flag as its own column, and remove it from the names.
    tb["sampling_standards_not_met"] = tb["country"].str.endswith(FLAG_SAMPLING_STANDARDS)
    tb["country"] = tb["country"].str.rstrip(FLAG_SAMPLING_STANDARDS).str.strip()
    tb["sampling_standards_not_met"] = tb.groupby("country")["sampling_standards_not_met"].transform("any")
    tb = tb.format(["country", "year"], short_name="pisa")

    # Step 3: Read the PISA Data Explorer exports of the previous version, as they were processed then.
    tb_explorer = read_data_explorer_exports()

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb, tb_explorer], default_metadata=snap_all.metadata)
    ds_meadow.save()


def read_cycle_table(snap, sheet: str, column: str, suffix: str = "") -> Table:
    """Read the mean score and standard error for each PISA cycle from one annex table.

    In every table, a header row names each cycle ("PISA 2000", ...) above its mean score, and the standard error is
    in the next column. Columns with longer headers ("PISA 2000 (PISA 2025 - PISA 2000)") are score changes, not
    scores, and are skipped.
    """
    tb_raw = snap.read_excel(sheet_name=sheet, header=None)
    rows = tb_raw.astype(object).where(tb_raw.notna(), None).values.tolist()
    check_pisa_for_development_note(rows, sheet)

    # Find the header row with the PISA cycles, and the columns of each cycle's mean score.
    header = next(i for i, row in enumerate(rows) if any(_is_cycle(cell) for cell in row))
    cycles = {j: int(cell[5:]) for j, cell in enumerate(rows[header]) if _is_cycle(cell)}
    # The last header row labels the standard errors, one column right of each mean.
    labels = next(row for row in rows[header + 1 :] if "S.E." in row)
    first_data_row = rows.index(labels) + 1
    for j in cycles:
        assert labels[j] in ("Mean score", "Score"), f"{sheet}: unexpected label above the mean in column {j}."
        assert labels[j + 1] == "S.E.", f"{sheet}: no standard error next to the mean in column {j}."

    records = []
    for row in rows[first_data_row:]:
        country = row[0]
        values = [row[j] for j in cycles] + [row[j + 1] for j in cycles]
        # Skip blank rows and the notes below the table (no numbers and no missing-data markers).
        if not isinstance(country, str) or not any(_is_value(v) for v in values):
            continue
        for j, year in cycles.items():
            records.append(
                {
                    "country": country.strip(),
                    "year": year,
                    f"{column}_average{suffix}": _to_number(row[j]),
                    f"{column}_se{suffix}": _to_number(row[j + 1]),
                }
            )
    tb = snap.read_from_records(records)
    tb = tb.dropna(subset=[f"{column}_average{suffix}"])
    return tb


def check_pisa_for_development_note(rows: list, sheet: str) -> None:
    """Assert the table note on PISA for Development lists the expected countries (garden relies on it)."""
    notes = [row[0] for row in rows if isinstance(row[0], str) and PISA_FOR_DEVELOPMENT_NOTE.search(row[0])]
    assert len(notes) == 1, f"{sheet}: note on PISA for Development not found."
    countries = PISA_FOR_DEVELOPMENT_NOTE.search(notes[0]).group(1)
    countries = {c.strip() for c in re.split(r",| and ", countries) if c.strip()}
    assert countries == EXPECTED_PISA_FOR_DEVELOPMENT, f"{sheet}: PISA for Development countries changed: {countries}."


def read_data_explorer_exports() -> Table:
    """Read the six PISA Data Explorer exports (up to PISA 2022), with the processing of the previous version."""
    # Mean scores of all students.
    tbs_all = []
    for subject in ["math", "science", "reading"]:
        snap = paths.load_snapshot(f"pisa_{subject}_all.xls")
        tb = _read_data_explorer_export(snap)
        tb = tb.rename(
            columns={"Average": f"pisa_{subject}_all_average", "Standard Error": f"pisa_{subject}_all_se"},
            errors="raise",
        )
        tb = tb.dropna(subset=[f"pisa_{subject}_all_average", f"pisa_{subject}_all_se"])
        tbs_all.append(tb[["country", "year", f"pisa_{subject}_all_average", f"pisa_{subject}_all_se"]])

    # Mean scores of girls and boys (the export lists girls first, then boys).
    tbs_gender = []
    for subject in ["math", "science", "reading"]:
        snap = paths.load_snapshot(f"pisa_{subject}_boys_girls.xls")
        tb = _read_data_explorer_export(snap)
        prefix = f"pisa_{subject}_boys_girls"
        tb = tb.rename(
            columns={
                "Average": f"{prefix}_average_girls",
                "Standard Error": f"{prefix}_se_girls",
                "Average.1": f"{prefix}_average_boys",
                "Standard Error.1": f"{prefix}_se_boys",
            },
            errors="raise",
        )
        columns = [f"{prefix}_average_boys", f"{prefix}_average_girls", f"{prefix}_se_girls", f"{prefix}_se_boys"]
        tb = tb.dropna(subset=columns)
        tbs_gender.append(tb[["country", "year"] + columns])

    tb = reduce(lambda left, right: pr.merge(left, right, on=["country", "year"], how="outer"), tbs_all + tbs_gender)
    tb = tb.format(["country", "year"], short_name="pisa_data_explorer")
    return tb


def _read_data_explorer_export(snap) -> Table:
    """Read one PISA Data Explorer export: the table starts on row 12, and "—" or "†" mark missing values."""
    tb = snap.read_excel(header=11)
    # The year is only given on the first row of each cycle.
    tb["Year/Study"] = tb["Year/Study"].ffill()
    tb = tb.replace(["—", "†"], np.nan)
    tb = tb.rename(columns={"Year/Study": "year", "Jurisdiction": "country"}, errors="raise")
    tb = tb.dropna(subset=["country"])
    return tb


def _is_cycle(cell) -> bool:
    return isinstance(cell, str) and re.fullmatch(r"PISA \d{4}", cell.strip()) is not None


def _is_value(cell) -> bool:
    return isinstance(cell, (int, float)) or (isinstance(cell, str) and cell.strip() in MISSING_MARKERS)


def _to_number(cell) -> float | None:
    return float(cell) if isinstance(cell, (int, float)) else None
