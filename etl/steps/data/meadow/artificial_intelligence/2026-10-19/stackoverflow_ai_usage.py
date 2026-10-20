"""Load Stack Overflow Developer Survey responses on AI tool usage into meadow.

Each survey year is a separate snapshot of Stack Overflow's public response data. From each, we keep only the columns
needed to compute the share of professional developers who use AI tools:
    - AISelect: the AI tool usage question.
    - MainBranch (2023-2025) or CodeMain (2026): the question used to identify professional developers.
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep from each survey year (the 2026 survey replaced MainBranch with CodeMain).
COLUMNS = {
    2023: ["MainBranch", "AISelect"],
    2024: ["MainBranch", "AISelect"],
    2025: ["MainBranch", "AISelect"],
    2026: ["CodeMain", "AISelect"],
}


def run() -> None:
    """Create meadow dataset."""
    #
    # Load inputs.
    #
    tables = []
    for year, columns in COLUMNS.items():
        snap = paths.load_snapshot(f"stackoverflow_developer_survey_{year}.csv")
        tb_year = snap.read_csv(usecols=columns, encoding="utf-8-sig", dtype="str")
        tb_year["year"] = year
        tables.append(tb_year)

    #
    # Process data.
    #
    # Combine all years (columns missing in a given year are left empty).
    tb = pr.concat(tables, ignore_index=True)

    # Responses have no identifier we keep, so number them within each survey year to build a unique key.
    tb["respondent"] = tb.groupby("year").cumcount()

    # Use snake_case column names, and categoricals for the (low-cardinality) answer columns.
    tb = tb.rename(columns={"MainBranch": "main_branch", "CodeMain": "code_main", "AISelect": "ai_select"})
    for column in ["main_branch", "code_main", "ai_select"]:
        tb[column] = tb[column].astype("category")

    tb = tb.format(["year", "respondent"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
