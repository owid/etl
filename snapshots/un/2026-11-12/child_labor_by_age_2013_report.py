"""Extract Table 5 (children in employment, child labour and hazardous work by age group, 2000-2012) from the ILO
2013 report "Marking progress against child labour - Global estimates and trends 2000-2012".

The table is only published inside the report PDF, so this script downloads the PDF, reads the text of the page
that holds the table, and parses its rows. Values are stored as published: numbers in thousands and shares in %.
"""

import re
import tempfile
from pathlib import Path

import pandas as pd
import pdfplumber
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Page of the PDF (0-indexed) that holds Table 5.
TABLE_PAGE_INDEX = 17

# Start of the title line that opens the table.
TABLE_TITLE = "Table 5."

# A table row: an optional age group label (only on the first row of each group), the year, and three pairs of
# (number in thousands, share in %) for children in employment, child labour and hazardous work.
ROW_PATTERN = re.compile(
    r"^(?:(?P<age_group>\d+-\d+ years) )?(?P<year>\d{4}) "
    r"(?P<employment_no>[\d,]+) (?P<employment_pct>[\d.]+) "
    r"(?P<child_labour_no>[\d,]+) (?P<child_labour_pct>[\d.]+) "
    r"(?P<hazardous_work_no>[\d,]+) (?P<hazardous_work_pct>[\d.]+)$"
)

# Rows the table holds: 2012 for ages 5-11 and 12-14, and 2000, 2004, 2008 and 2012 for ages 5-14 and 15-17.
EXPECTED_ROWS = 10


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()

    # Download the report and read the text of the page with the table.
    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_path = Path(tmp_dir) / "report.pdf"
        response = requests.get(snap.metadata.origin.url_download, timeout=60)  # type: ignore
        response.raise_for_status()
        pdf_path.write_bytes(response.content)
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[TABLE_PAGE_INDEX].extract_text()

    # Parse the table rows.
    df = _parse_table(text)

    # Save snapshot.
    snap.create_snapshot(data=df, upload=upload)


def _parse_table(text: str) -> pd.DataFrame:
    """Parse the rows of Table 5 from the page text."""
    lines = [line.strip() for line in text.splitlines()]
    title_lines = [i for i, line in enumerate(lines) if line.startswith(TABLE_TITLE)]
    assert len(title_lines) == 1, "Table 5 title not found on the expected page; the PDF layout may have changed."

    # Read rows from the title down, skipping the header lines, until the first line after the table.
    rows = []
    age_group = None
    for line in lines[title_lines[0] + 1 :]:
        match = ROW_PATTERN.match(line)
        if match is None:
            if rows:
                break
            continue
        row = match.groupdict()
        # The age group label is only printed on the first row of each group.
        age_group = row["age_group"] or age_group
        row["age_group"] = age_group
        rows.append(row)

    df = pd.DataFrame(rows)
    num_cols = [c for c in df.columns if c.endswith("_no")]
    pct_cols = [c for c in df.columns if c.endswith("_pct")]
    df[num_cols] = df[num_cols].apply(lambda col: col.str.replace(",", "", regex=False).astype(int))
    df[pct_cols] = df[pct_cols].astype(float)
    df["year"] = df["year"].astype(int)

    # Sanity checks on the parsed table.
    assert len(df) == EXPECTED_ROWS, f"Expected {EXPECTED_ROWS} rows in Table 5, got {len(df)}."
    assert set(df["age_group"]) == {"5-11 years", "12-14 years", "5-14 years", "15-17 years"}
    assert set(df["year"]) == {2000, 2004, 2008, 2012}
    # Child labour is a subset of employment, and hazardous work a subset of child labour.
    assert (df["employment_no"] >= df["child_labour_no"]).all()
    assert (df["child_labour_no"] >= df["hazardous_work_no"]).all()
    # For ages 15-17, child labour is hazardous work by definition.
    df_15_17 = df[df["age_group"] == "15-17 years"]
    assert (df_15_17["child_labour_no"] == df_15_17["hazardous_work_no"]).all()
    # In 2012, ages 5-11 and 12-14 add up to ages 5-14.
    df_2012 = df[df["year"] == 2012].set_index("age_group")
    for col in ["child_labour_no", "hazardous_work_no"]:
        parts = int(df_2012.loc["5-11 years", col]) + int(df_2012.loc["12-14 years", col])
        assert parts == int(df_2012.loc["5-14 years", col]), f"2012 ages 5-11 and 12-14 don't add up to 5-14 for {col}."

    return df
