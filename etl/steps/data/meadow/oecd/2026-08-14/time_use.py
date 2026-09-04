"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets of the OECD workbook, and the sex label each one reports on.
SHEETS = {"Total": "total", "Men": "men", "Women": "women"}

# The markers the source writes in place of a number. All become NaN, but they do not mean the same
# thing, and the set is asserted rather than assumed: ".." and a blank are "not available", "-" is
# not applicable, and "(see notes)" means the minutes were counted under a *different* activity and a
# footnote says which — Japan's household travel, which the source folds into its "other" category.
# A marker the source adds in a later edition would otherwise become a silent NaN, and every group
# that is built as a remainder would quietly absorb it.
MISSING_MARKERS = {"..", "-", "(see notes)"}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("time_use.xlsx")

    # Parse each sheet (total, men, women) and combine them into one long table.
    tables = [parse_sheet(snap.read_excel(sheet_name=sheet, header=None), sex=sex) for sheet, sex in SHEETS.items()]
    tb = pr.concat(tables, ignore_index=True)

    for column in ["country", "sex", "activity_code", "activity", "survey_year", "age_of_reference"]:
        tb[column] = tb[column].astype("category")

    tb = tb.format(["country", "sex", "activity_code"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()


def parse_sheet(tb: Table, sex: str) -> Table:
    """Parse one sheet of the OECD workbook into a long table.

    Each sheet has three header rows (country, survey year, age of reference) followed by one row
    per activity, with the activity code in the first column and its label in the second. Rows
    without an activity code (footnotes, blanks) are dropped, and so are columns without a country
    header (spacers, plus one stray far-right cell in the source file).
    """
    header, survey_year, age_of_reference = tb.iloc[0], tb.iloc[1], tb.iloc[2]
    country_columns = [
        column for column in tb.columns[2:] if isinstance(header[column], str) and header[column].strip()
    ]

    data = tb.iloc[3:]
    data = data[data[tb.columns[0]].notna()]
    data = data[[tb.columns[0], tb.columns[1]] + country_columns].rename(
        columns={tb.columns[0]: "activity_code", tb.columns[1]: "activity"}
    )

    long = data.melt(id_vars=["activity_code", "activity"], var_name="_column", value_name="minutes")
    # Footnote markers ("*", "**") and stray whitespace are dropped from country labels.
    long["country"] = long["_column"].map({c: header[c].replace("*", "").strip() for c in country_columns})
    long["survey_year"] = long["_column"].map({c: str(survey_year[c]).strip() for c in country_columns})
    long["age_of_reference"] = long["_column"].map({c: str(age_of_reference[c]).strip() for c in country_columns})
    long = long.drop(columns=["_column"])

    long["activity_code"] = long["activity_code"].astype(str).str.strip()
    long["activity"] = long["activity"].astype(str).str.strip()
    long["sex"] = sex
    markers = {str(value).strip() for value in long["minutes"].dropna().unique() if not isinstance(value, (int, float))}
    assert markers <= MISSING_MARKERS, (
        f"Unknown missing-value markers in the {sex} sheet: {sorted(markers - MISSING_MARKERS)}"
    )
    long["minutes"] = pr.to_numeric(long["minutes"], errors="coerce")

    # Label columns built via .map/.str lose the snapshot origins; restore them from the values.
    for column in ["activity", "survey_year", "age_of_reference"]:
        long[column] = long[column].copy_metadata(long["minutes"])

    return long
