"""Snapshot of deaths, population and crude death rates by cause of death in the United States, from CDC WONDER.

CDC WONDER's "Underlying Cause of Death" databases are queried through their XML API, for the whole United
States, one row per year and cause in NCHS's list of 113 selected causes of death, plus one row per year for all
causes. Two databases are used, so that each year comes from the most recent one:
- D76 (1999–2020, bridged-race population estimates) for 1999–2017.
- D158 (2018 onwards, single-race population estimates) for 2018 onwards.

The rows are stored as WONDER returns them, with its cause labels and its markers ("Unreliable", "Suppressed").

NOTE: Update LATEST_YEAR when WONDER adds a new year of final data to D158.
"""

import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

URL = "https://wonder.cdc.gov/controller/datarequest/{database}"

# Years queried from each database.
YEARS_D76 = list(range(1999, 2018))
LATEST_YEAR = 2024
YEARS_D158 = list(range(2018, LATEST_YEAR + 1))

# WONDER asks API users to leave time between requests.
SECONDS_BETWEEN_REQUESTS = 30

# Label of the rows with the total for all causes (WONDER's own name for that selection).
ALL_CAUSES_LABEL = "All Causes of Death"


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()

    # Query each database by cause and in total, and collect the rows.
    rows = []
    for database, years in [("D76", YEARS_D76), ("D158", YEARS_D158)]:
        for by_cause in [True, False]:
            xml = query_wonder(database=database, years=years, by_cause=by_cause)
            rows += parse_response(xml, database=database, by_cause=by_cause)
            time.sleep(SECONDS_BETWEEN_REQUESTS)

    df = pd.DataFrame(rows)

    # Check that every year has a total and the full list of causes.
    years_expected = set(YEARS_D76) | set(YEARS_D158)
    assert set(df["year"].astype(int)) == years_expected, "Some years are missing from the WONDER responses."
    causes_per_year = df[df["cause"] != ALL_CAUSES_LABEL].groupby("year")["cause"].nunique()
    assert causes_per_year.min() >= 113, f"Fewer causes than expected in some years: {causes_per_year.min()}."
    totals = df[df["cause"] == ALL_CAUSES_LABEL]
    assert len(totals) == len(years_expected), "Expected exactly one all-causes row per year."

    snap.create_snapshot(data=df, upload=upload)


def query_wonder(database: str, years: list[int], by_cause: bool) -> str:
    """Send one request to the WONDER API and return the XML response."""
    response = requests.post(
        URL.format(database=database),
        data={
            "request_xml": build_request(database=database, years=years, by_cause=by_cause),
            "accept_datause_restrictions": "true",
        },
        timeout=600,
    )
    response.raise_for_status()
    return response.text


def build_request(database: str, years: list[int], by_cause: bool) -> str:
    """Build the request: deaths, population and crude rate for the whole US, by year (and by cause if requested)."""
    db = database
    params: dict[str, list[str]] = {
        # Group by year, and by NCHS's list of 113 selected causes.
        "B_1": [f"{db}.V1-level1"],
        "B_2": [f"{db}.V4" if by_cause else "*None*"],
        "B_3": ["*None*"],
        "B_4": ["*None*"],
        "B_5": ["*None*"],
        # Measures: deaths, population, crude rate.
        "M_1": [f"{db}.M1"],
        "M_2": [f"{db}.M2"],
        "M_3": [f"{db}.M3"],
        # Filters: the selected years, all causes, the whole country.
        f"F_{db}.V1": [str(year) for year in years],
        f"F_{db}.V2": ["*All*"],
        f"F_{db}.V9": ["*All*"],
        f"F_{db}.V10": ["*All*"],
        f"F_{db}.V27": ["*All*"],
        f"I_{db}.V1": ["\n".join(f"{year} ({year})" for year in years) + "\n"],
        f"I_{db}.V2": ["*All* (All Causes of Death)\n"],
        f"I_{db}.V9": ["*All* (The United States)\n"],
        f"I_{db}.V10": ["*All* (The United States)\n"],
        f"I_{db}.V27": ["*All* (The United States)\n"],
        # No other restriction (age, sex, race, place of death...).
        **{f"V_{db}.{v}": ["*All*"] for v in ["V4", "V5", "V7", "V11", "V12", "V17", "V19", "V20", "V21", "V22"]},
        **{f"V_{db}.{v}": ["*All*"] for v in ["V23", "V24", "V51", "V52"]},
        **{f"V_{db}.{v}": [""] for v in ["V1", "V2", "V9", "V10", "V27"]},
        f"V_{db}.V6": ["00"],
        # Options.
        **{f"O_{v}_fmode": ["freg"] for v in ["V1", "V2", "V4", "V9", "V10", "V27"]},
        "O_aar": ["aar_none"],
        "O_aar_pop": ["0000"],
        "O_age": [f"{db}.V5"],
        "O_javascript": ["on"],
        "O_location": [f"{db}.V9"],
        "O_precision": ["1"],
        "O_rate_per": ["100000"],
        "O_show_suppressed": ["true"],
        "O_show_totals": ["false"],
        "O_show_zeros": ["true"],
        "O_timeout": ["600"],
        "O_title": ["owid"],
        "O_ucd": [f"{db}.V4"],
        "O_urban": [f"{db}.V19"],
        "action-Send": ["Send"],
        "dataset_code": [db],
        "stage": ["request"],
    }
    # The race variable differs between the bridged-race (D76) and single-race (D158) databases.
    if db == "D158":
        params["O_race"] = [f"{db}.V42"]
        params[f"V_{db}.V42"] = ["*All*"]
    else:
        params[f"V_{db}.V8"] = ["*All*"]

    root = ET.Element("request-parameters")
    for name, values in params.items():
        parameter = ET.SubElement(root, "parameter")
        ET.SubElement(parameter, "name").text = name
        for value in values:
            ET.SubElement(parameter, "value").text = value
    return ET.tostring(root, encoding="unicode")


def parse_response(xml: str, database: str, by_cause: bool) -> list[dict]:
    """Read the data rows of a WONDER response, keeping its labels and values as text."""
    root = ET.fromstring(xml)
    rows = []
    for r in root.iter("r"):
        labels = [c.get("l") for c in r if c.get("l") is not None]
        values = [c.get("v") for c in r if c.get("v") is not None]
        # Skip total rows (they carry "dt" values instead of "v").
        if not labels:
            continue
        expected = (2, 3) if by_cause else (1, 3)
        assert (len(labels), len(values)) == expected, f"Unexpected row layout in {database}: {ET.tostring(r)!r}"
        rows.append(
            {
                "database": database,
                "year": labels[0].strip(),
                "cause": labels[1] if by_cause else ALL_CAUSES_LABEL,
                "deaths": values[0],
                "population": values[1],
                "crude_rate": values[2],
            }
        )
    assert rows, f"No data rows in the {database} response."
    return rows
