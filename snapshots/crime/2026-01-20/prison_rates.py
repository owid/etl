"""Script to create a snapshot of dataset."""

import re
from io import StringIO
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.helpers import PathFinder

paths = PathFinder(__file__)
BASE = "https://www.prisonstudies.org/country/"


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    all_data = []
    countries = get_country_names()
    for country_info in countries:
        country_df = fetch_wpb_country(country_info["slug"], country_info["name"])
        all_data.append(country_df)

    combined_data = pd.concat(all_data, ignore_index=True)

    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=combined_data)


def get_country_names() -> List[dict]:
    """Get list of countries with both name and slug."""
    url = "https://www.prisonstudies.org/highest-to-lowest/prison-population-total?field_region_taxonomy_tid=All"

    df = pd.read_html(url)[0]

    countries = []
    for name in df["Title"].dropna():
        slug = (
            name.lower()
            .replace(r"[()/:,&.'']", "")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "")
            .replace(":", "")
            .replace(",", "")
            .replace("&", "")
            .replace(".", "")
            .replace("'", "")
        )
        # Remove "of" and "the"
        slug = " ".join(word for word in slug.split() if word not in ["of", "the"])
        # Replace spaces with hyphens
        slug = "-".join(slug.split())
        slug = slug.strip("-")

        countries.append({"name": name, "slug": slug})

    return countries


def _clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _extract_value_and_date(block_text: str):
    """
    Extract (value, date) from WPB strings.

    Handles:
      - "40 544 at 31.12.2024 (...) Prison ..."
      - "10 599 at 2004 (...) Pris"
      - "60 based on ... at mid-2004 (United Nations)"
    """
    if not block_text:
        return None, None

    t = _clean_space(block_text)

    # Remove leading "c." / "circa"
    t = re.sub(r"^(c\.|circa)\s*", "", t, flags=re.IGNORECASE)

    # 1) Extract a leading numeric-ish value (first number on the string)
    m_val = re.search(r"^(?P<value>\d[\d\s,\.]*)\b", t)
    if not m_val:
        return None, None
    value = m_val.group("value").strip()

    # Optional: strip trailing label fragments to reduce false matches
    # (but don't cut too aggressively; keep text after value for date search)
    # We'll work with the full string for date extraction.

    # 2) First, try the most reliable: "value at <date>" right after value
    after_value = t[m_val.end() :].strip()

    # a) at DD.MM.YYYY
    m = re.search(r"^at\s+(\d{2}\.\d{2}\.\d{4})\b", after_value)
    if m:
        return value, m.group(1)

    # b) at Month YYYY
    m = re.search(r"^at\s+([A-Za-z]+\s+\d{4})\b", after_value)
    if m:
        return value, m.group(1)

    # c) at YYYY
    m = re.search(r"^at\s+((19|20)\d{2})\b", after_value)
    if m:
        return value, m.group(1)

    # 3) If not immediately after value, search anywhere later in the string,
    #    preferring specific "at ..." date phrases.
    # a) at mid-2004 / end-2004 / start-2004
    m = re.search(r"\bat\s+(?:mid|end|start)[-\s]((19|20)\d{2})\b", t, flags=re.IGNORECASE)
    if m:
        return value, m.group(1)

    # b) at 31.12.2024 (anywhere)
    m = re.search(r"\bat\s+(\d{2}\.\d{2}\.\d{4})\b", t)
    if m:
        return value, m.group(1)

    # c) at Month YYYY (anywhere)
    m = re.search(r"\bat\s+([A-Za-z]+\s+\d{4})\b", t)
    if m:
        return value, m.group(1)

    # d) at YYYY (anywhere)
    m = re.search(r"\bat\s+((19|20)\d{2})\b", t)
    if m:
        return value, m.group(1)

    # 4) Last resort: any year anywhere (take the first plausible one after value)
    m = re.search(r"\b((19|20)\d{2})\b", t[m_val.end() :])
    if m:
        return value, m.group(1)

    return value, None


def _to_number(s: str):
    """
    Convert strings like '94 749', 'c. 18,000', '119.0%', 'Circa 79 620' to float/int-ish.
    Returns None if not parsable.
    """
    if s is None:
        return None
    t = s.lower()
    t = t.replace("circa", "").replace("c.", "").replace("%", "").strip()
    t = t.replace(",", "").replace("\u00a0", " ")  # nbsp
    # keep digits/spaces/dot
    t2 = re.sub(r"[^0-9.\s-]", "", t).strip()
    t2 = t2.replace(" ", "")
    if not t2:
        return None
    # int if possible
    try:
        f = float(t2)
        if f.is_integer():
            return int(f)
        return f
    except ValueError:
        return None


# --- main ------------------------------------------------------------------


def _get_overview_container(soup: BeautifulSoup):
    """
    Return the BeautifulSoup node containing the Overview tab content.
    Falls back to the full soup if not found.
    """
    # Common patterns on WPB pages (Bootstrap-ish tabs)
    return (
        soup.select_one(".tab-pane.active")  # currently active tab pane
        or soup.select_one("#overview")  # if an explicit id exists
        or soup.select_one("[id*='overview']")  # fallback
        or soup
    )


def _clean_trend_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize a prison population trend table."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Clean year and sort
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df = df.dropna(subset=["Year"]).sort_values("Year")

    # Clean numeric-ish strings (handles commas, spaces, and "c.")
    for col in ["Prison population total", "Prison population rate"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"^\s*(c\.|circa)\s*", "", regex=True)  # remove leading "c." / "circa"
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Rename to match snapshot naming convention
    df = df.rename(
        columns={
            "Year": "year",
            "Prison population total": "prison_population_total",
            "Prison population rate": "prison_population_rate",
        }
    )

    return df.drop_duplicates().reset_index(drop=True)


def fetch_wpb_country(country_slug: str, country_name: str):
    """
    Fetch prison data for a country from World Prison Brief.

    Args:
        country_slug: URL slug for the country
        country_name: Actual country name to use in the data

    Returns:
      combined_df: dataframe with both snapshot and trend data, with aligned column names
    """
    url = BASE + country_slug
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text("\n")

    # Extract current snapshot statistics
    labels = {
        "prison_population_total": "Prison population total (including pre-trial detainees / remand prisoners)",
        "prison_population_rate": "Prison population rate (per 100,000 of national population)",
        "pretrial_percent": "Pre-trial detainees / remand prisoners (percentage of prison population)",
        "female_percent": "Female prisoners (percentage of prison population)",
        "juveniles_percent": "Juveniles / minors / young prisoners incl. definition (percentage of prison population)",
        "foreign_prisoners": "Foreign prisoners (percentage of prison population)",
        "institutions_count": "Number of establishments / institutions",
        "official_capacity": "Official capacity of prison system",
        "occupancy_level": "Occupancy level (based on official capacity)",
    }

    snapshot = {}

    for key, label in labels.items():
        idx = page_text.find(label)
        if idx == -1:
            snapshot[key] = {"value_raw": None, "value": None, "date": None}
            continue

        window = page_text[idx : idx + 400]
        after = window.split(label, 1)[-1].strip()
        lines = [ln.strip() for ln in after.splitlines() if ln.strip()]
        candidate = " ".join(lines[:3])

        value_raw, date = _extract_value_and_date(candidate)
        snapshot[key] = {
            "value_raw": value_raw,
            "value": _to_number(value_raw),
            "date": date,
        }

    # Convert snapshot date strings to years for matching
    def _extract_year(date_str):
        """Extract year from various date formats."""
        if date_str is None:
            return None
        # Match 4-digit year
        match = re.search(r"(19|20)\d{2}", str(date_str))
        return int(match.group(0)) if match else None

    snapshot_df = pd.DataFrame(
        [
            {
                "country": country_name,
                "year": _extract_year(snapshot["prison_population_total"]["date"]),
                "prison_population_total": snapshot["prison_population_total"]["value"],
                "prison_population_rate": snapshot["prison_population_rate"]["value"],
                "pretrial_detainees_pct": snapshot["pretrial_percent"]["value"],
                "female_prisoners_pct": snapshot["female_percent"]["value"],
                "juvenile_prisoners_pct": snapshot["juveniles_percent"]["value"],
                "foreign_prisoners_pct": snapshot["foreign_prisoners"]["value"],
                "number_of_institutions": snapshot["institutions_count"]["value"],
                "official_capacity": snapshot["official_capacity"]["value"],
                "occupancy_level_pct": snapshot["occupancy_level"]["value"],
            }
        ]
    )

    # Extract trend tables from Overview section
    trend_dfs = []
    overview = _get_overview_container(soup)
    tables = pd.read_html(StringIO(str(overview)))

    for i, df in enumerate(tables):
        # Normalize column names for checking
        df_check = df.copy()
        df_check.columns = [str(c).strip() for c in df_check.columns]
        cols = set(df_check.columns)

        # Check if this is a prison population trend table by column names
        if {"Year", "Prison population total", "Prison population rate"}.issubset(cols):
            df_clean = _clean_trend_table(df)
            df_clean["country"] = country_name
            trend_dfs.append(df_clean)

        # Alternative: check if first cell contains "Prison population trend"
        # (handles tables where the header is in the first row)
        elif df.shape[1] >= 3 and df.shape[0] > 1:
            first_cell = str(df.iloc[0, 0]).lower() if not df.empty else ""
            if "prison population trend" in first_cell:
                # Skip header row and use numeric columns
                df_trend = df.iloc[1:].copy()
                df_trend.columns = ["Year", "Prison population total", "Prison population rate"]
                df_clean = _clean_trend_table(df_trend)
                df_clean["country"] = country_name
                trend_dfs.append(df_clean)

    # Concatenate all trend dataframes
    trend_df = pd.concat(trend_dfs, ignore_index=True) if trend_dfs else pd.DataFrame()

    # Combine snapshot and trend data
    if not trend_df.empty:
        combined_df = pd.concat([trend_df, snapshot_df], ignore_index=True)
    else:
        combined_df = snapshot_df

    # Sort by country and year
    if "year" in combined_df.columns:
        combined_df = combined_df.sort_values(["country", "year"]).reset_index(drop=True)

    return combined_df
