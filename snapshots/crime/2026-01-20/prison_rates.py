"""Script to create a snapshot of dataset."""

import re
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.helpers import PathFinder

paths = PathFinder(__file__)
BASE = "https://www.prisonstudies.org/country/"


country_harmonizer = {"Sint-Maarten-(Netherlands)"}


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    countries = get_country_names()
    for country in countries:
        print(country)
        snapshot, trends = fetch_wpb_country(country)
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot.
    snap.create_snapshot(upload=upload)


def get_country_names() -> List[str]:
    url = "https://www.prisonstudies.org/highest-to-lowest/prison-population-total?field_region_taxonomy_tid=All"

    # Read the first table on the page
    df = pd.read_html(url)[0]

    # The country/territory names are in the "Title" column
    countries = df["Title"].dropna().tolist()
    countries = [c.replace(" ", "-") for c in countries]
    countries = [c.replace("(", "") for c in countries]
    countries = [c.replace(")", "") for c in countries]
    countries = [c.replace("-of", "") for c in countries]
    countries = [c.replace("/", "") for c in countries]
    countries = [c.replace(":", "") for c in countries]
    countries = [c.replace("&", "") for c in countries]

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


def fetch_wpb_country(country_slug: str):
    """
    Returns:
      snapshot_df: one-row dataframe with value + date columns
      trend_dfs: dict of extracted trend tables (prison population trend, and possibly "up to 2000")
    """
    url = BASE + country_slug
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text("\n")

    # We’ll key off the exact labels used on the page.
    # (If WPB tweaks wording slightly, you can add variants here.)
    labels = {
        "prison_population_total": "Prison population total (including pre-trial detainees / remand prisoners)",
        "prison_population_rate": "Prison population rate (per 100,000 of national population)",
        "pretrial_percent": "Pre-trial detainees / remand prisoners (percentage of prison population)",
        "female_percent": "Female prisoners (percentage of prison population)",
        "institutions_count": "Number of establishments / institutions",
        "official_capacity": "Official capacity of prison system",
        "occupancy_level": "Occupancy level (based on official capacity)",
    }

    snapshot = {}

    # Strategy:
    # Find each label in the raw page text and capture the next ~250 chars.
    # This is surprisingly robust for WPB pages because the label and its value/date are close.
    for key, label in labels.items():
        idx = page_text.find(label)
        if idx == -1:
            snapshot[key] = {"value_raw": None, "value": None, "date": None}
            continue

        window = page_text[idx : idx + 400]  # enough to include value + date
        # remove the label itself, keep what's after it
        after = window.split(label, 1)[-1].strip()

        # value tends to be on the next non-empty lines; take first few lines
        lines = [ln.strip() for ln in after.splitlines() if ln.strip()]
        candidate = " ".join(lines[:3])  # value + date usually fit here

        value_raw, date = _extract_value_and_date(candidate)
        snapshot[key] = {
            "value_raw": value_raw,
            "value": _to_number(value_raw),
            "date": date,
        }

    snapshot_df = pd.DataFrame(
        [
            {
                "country_slug": country_slug,
                "prison_population_total": snapshot["prison_population_total"]["value"],
                "prison_population_total_date": snapshot["prison_population_total"]["date"],
                "prison_population_rate": snapshot["prison_population_rate"]["value"],
                "prison_population_rate_date": snapshot["prison_population_rate"]["date"],  # may be None on some pages
                "pretrial_detainees_pct": snapshot["pretrial_percent"]["value"],
                "pretrial_detainees_pct_date": snapshot["pretrial_percent"]["date"],
                "female_prisoners_pct": snapshot["female_percent"]["value"],
                "female_prisoners_pct_date": snapshot["female_percent"]["date"],
                "number_of_institutions": snapshot["institutions_count"]["value"],
                "number_of_institutions_date": snapshot["institutions_count"]["date"],
                "official_capacity": snapshot["official_capacity"]["value"],
                "official_capacity_date": snapshot["official_capacity"]["date"],
                "occupancy_level_pct": snapshot["occupancy_level"]["value"],
                "occupancy_level_pct_date": snapshot["occupancy_level"]["date"],
            }
        ]
    )

    # --- trend tables -------------------------------------------------------
    # Use read_html to pull all tables; then pick the ones we want by column names.
    tables = pd.read_html(r.text)
    trend_dfs = {}

    def normalise_cols(df):
        df2 = df.copy()
        df2.columns = [str(c).strip() for c in df2.columns]
        return df2

    for i, df in enumerate(tables):
        df = normalise_cols(df)

        # Prison population trend table: usually columns like
        # Year | Prison population total | Prison population rate
        cols = set(df.columns)
        if {"Year", "Prison population total", "Prison population rate"}.issubset(cols):
            # Some pages duplicate the same table; de-duplicate by content
            df2 = df.copy()
            df2["Year"] = pd.to_numeric(df2["Year"], errors="coerce")
            df2 = df2.dropna(subset=["Year"]).sort_values("Year")
            df2 = df2.drop_duplicates()
            # Convert numeric-ish strings
            for c in ["Prison population total", "Prison population rate"]:
                df2[c] = (
                    df2[c]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("c", "", regex=False)
                    .str.replace(" ", "", regex=False)
                )
                df2[c] = pd.to_numeric(df2[c], errors="coerce")
            # If we already captured one, store additional ones under different keys
            key = (
                "prison_population_trend"
                if "prison_population_trend" not in trend_dfs
                else f"prison_population_trend_{i}"
            )
            trend_dfs[key] = df2.reset_index(drop=True)

    return snapshot_df, trend_dfs


# Example:
# snapshot, trends = fetch_wpb_country("algeria")
# print(snapshot)
# print(trends["prison_population_trend"].tail())
