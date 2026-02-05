"""Script to create a snapshot of dataset."""

import re
from datetime import datetime
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

    # a) at DD.MM.YYYY or DD.MM.YY
    m = re.search(r"^at\s+(\d{2}\.\d{2}\.\d{2,4})\b", after_value)
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

    # b) at 31.12.2024 or 31.12.24 (anywhere)
    m = re.search(r"\bat\s+(\d{2}\.\d{2}\.\d{2,4})\b", t)
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


def _validate_data(df: pd.DataFrame, country_name: str) -> None:
    """
    Validate prison data for quality and consistency.

    Raises warnings for data quality issues but does not fail the pipeline.
    """
    current_year = datetime.now().year

    # Check year range
    if "year" in df.columns:
        years = df["year"].dropna()
        if not years.empty:
            invalid_years = years[(years < 1990) | (years > current_year)]
            if not invalid_years.empty:
                print(
                    f"⚠️  WARNING [{country_name}]: Found {len(invalid_years)} years outside valid range (1990-{current_year}): {invalid_years.tolist()}"
                )

    # Check prison population total
    if "prison_population_total" in df.columns:
        pop = df["prison_population_total"].dropna()
        if not pop.empty:
            # Check for negative values
            negative = pop[pop < 0]
            if not negative.empty:
                print(f"⚠️  WARNING [{country_name}]: Found {len(negative)} negative prison population values")

            # Check for unreasonably high values (> 10 million)
            very_high = pop[pop > 10_000_000]
            if not very_high.empty:
                print(
                    f"⚠️  WARNING [{country_name}]: Found {len(very_high)} prison population values > 10 million: {very_high.tolist()}"
                )

    # Check prison population rate (per 100k)
    if "prison_population_rate" in df.columns:
        rate = df["prison_population_rate"].dropna()
        if not rate.empty:
            # Check for negative values
            negative = rate[rate < 0]
            if not negative.empty:
                print(f"⚠️  WARNING [{country_name}]: Found {len(negative)} negative prison population rate values")

            # Check for unreasonably high rates (> 1000 per 100k)
            very_high = rate[rate > 1000]
            if not very_high.empty:
                print(
                    f"⚠️  WARNING [{country_name}]: Found {len(very_high)} prison population rate values > 1000 per 100k: {very_high.tolist()}"
                )

    # Check percentage columns
    percentage_cols = [
        "pretrial_detainees_pct",
        "pretrial_remand_pct",
        "female_prisoners_pct",
        "juvenile_prisoners_pct",
        "foreign_prisoners_pct",
        "occupancy_level_pct",
    ]

    for col in percentage_cols:
        if col in df.columns:
            pct = df[col].dropna()
            if not pct.empty:
                # Check for negative percentages
                negative = pct[pct < 0]
                if not negative.empty:
                    print(f"⚠️  WARNING [{country_name}]: Found {len(negative)} negative values in {col}")

                # Check for percentages > 100% (allow some margin for occupancy)
                max_allowed = 300 if col == "occupancy_level_pct" else 100
                too_high = pct[pct > max_allowed]
                if not too_high.empty:
                    print(
                        f"⚠️  WARNING [{country_name}]: Found {len(too_high)} values > {max_allowed}% in {col}: {too_high.tolist()}"
                    )

    # Check that prison population rate is consistent with total
    if all(col in df.columns for col in ["prison_population_total", "prison_population_rate"]):
        # Only check rows where both values are present
        check_df = df.dropna(subset=["prison_population_total", "prison_population_rate"])
        if not check_df.empty:
            # Rate should be roughly: (total / country_population) * 100,000
            # We can't verify exactly without population data, but we can check if rate is zero when total is not
            zero_rate_nonzero_total = check_df[
                (check_df["prison_population_rate"] == 0) & (check_df["prison_population_total"] > 0)
            ]
            if not zero_rate_nonzero_total.empty:
                print(
                    f"⚠️  WARNING [{country_name}]: Found {len(zero_rate_nonzero_total)} rows with prison population but zero rate"
                )

    # Check official capacity vs actual population
    if all(col in df.columns for col in ["prison_population_total", "official_capacity"]):
        check_df = df.dropna(subset=["prison_population_total", "official_capacity"])
        if not check_df.empty:
            # Check if capacity is less than population (overcrowding indicator)
            overcrowded = check_df[check_df["official_capacity"] < check_df["prison_population_total"]]
            if not overcrowded.empty and len(overcrowded) == len(check_df):
                # All entries show overcrowding - this might be expected in some countries
                pass

            # Check for capacity being zero while population exists
            zero_capacity = check_df[(check_df["official_capacity"] == 0) & (check_df["prison_population_total"] > 0)]
            if not zero_capacity.empty:
                print(
                    f"⚠️  WARNING [{country_name}]: Found {len(zero_capacity)} rows with prison population but zero official capacity"
                )

    # Check pre-trial/remand number
    if "pretrial_remand_number" in df.columns:
        pretrial_num = df["pretrial_remand_number"].dropna()
        if not pretrial_num.empty:
            # Check for negative values
            negative = pretrial_num[pretrial_num < 0]
            if not negative.empty:
                print(f"⚠️  WARNING [{country_name}]: Found {len(negative)} negative pre-trial/remand number values")

            # Check if pre-trial number exceeds total prison population
            if "prison_population_total" in df.columns:
                check_df = df.dropna(subset=["pretrial_remand_number", "prison_population_total"])
                if not check_df.empty:
                    exceeds = check_df[
                        check_df["pretrial_remand_number"] > check_df["prison_population_total"]
                    ]
                    if not exceeds.empty:
                        print(
                            f"⚠️  WARNING [{country_name}]: Found {len(exceeds)} rows where pre-trial number exceeds total prison population"
                        )

    # Check pre-trial/remand rate (per 100k)
    if "pretrial_remand_rate" in df.columns:
        pretrial_rate = df["pretrial_remand_rate"].dropna()
        if not pretrial_rate.empty:
            # Check for negative values
            negative = pretrial_rate[pretrial_rate < 0]
            if not negative.empty:
                print(f"⚠️  WARNING [{country_name}]: Found {len(negative)} negative pre-trial/remand rate values")

            # Check for unreasonably high rates (> 500 per 100k)
            very_high = pretrial_rate[pretrial_rate > 500]
            if not very_high.empty:
                print(
                    f"⚠️  WARNING [{country_name}]: Found {len(very_high)} pre-trial/remand rate values > 500 per 100k: {very_high.tolist()}"
                )


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


def _clean_pretrial_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize a pre-trial/remand imprisonment table."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Clean year column
    if "Year" in df.columns:
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df = df.dropna(subset=["Year"]).sort_values("Year")

    # Clean numeric columns
    numeric_cols = {
        "Number in pre-trial/remand imprisonment": "pretrial_remand_number",
        "Percentage of total prison population": "pretrial_remand_pct",
        "Pre-trial/remand population rate (per 100,000 of national population)": "pretrial_remand_rate",
    }

    for orig_col, new_col in numeric_cols.items():
        if orig_col in df.columns:
            # Clean the column values
            df[orig_col] = (
                df[orig_col]
                .astype(str)
                .str.replace(r"^\s*(c\.|circa)\s*", "", regex=True)  # remove leading "c." / "circa"
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace("%", "", regex=False)  # remove percentage signs
                .str.strip()
            )
            df[orig_col] = pd.to_numeric(df[orig_col], errors="coerce")

    # Rename columns to match snapshot naming convention
    df = df.rename(
        columns={
            "Year": "year",
            **numeric_cols,
        }
    )

    return df.drop_duplicates().reset_index(drop=True)


def _extract_pretrial_table(soup: BeautifulSoup, country_name: str) -> pd.DataFrame:
    """
    Extract pre-trial/remand imprisonment table from country page.

    These tables have a special format where multiple values are in one cell separated by <br/> tags.
    Example structure:
      Year | Number | Percentage | Rate
      2000<br/>2005 | 349,800<br/>463,200 | 18.1%<br/>21.1% | 123<br/>156
    """
    # Find tables that contain "pre-trial/remand" text
    tables = soup.find_all("table")

    for table in tables:
        # Check if this table contains pre-trial/remand header
        table_text = table.get_text()
        if "pre-trial/remand" not in table_text.lower():
            continue

        # Get all rows
        rows = table.find_all("tr")
        if len(rows) < 2:  # Need at least header + data
            continue

        # Check if first row has the expected headers
        header_row = rows[0]
        header_text = header_row.get_text().lower()
        if "year" not in header_text or "number in" not in header_text:
            continue

        # Extract data from subsequent rows
        # Each cell may contain multiple values separated by <br/>
        all_years = []
        all_numbers = []
        all_percentages = []
        all_rates = []

        for row in rows[1:]:  # Skip header
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:  # Need at least 3 columns
                continue

            # Extract text from each cell, splitting by <br/>
            cell_values = []
            for cell in cells:
                # Get text with br tags preserved as separators
                text_parts = []
                for content in cell.descendants:
                    if isinstance(content, str):
                        text = content.strip()
                        if text:
                            text_parts.append(text)
                cell_values.append(text_parts)

            # Determine the maximum number of values in any cell (this is the number of rows)
            max_len = max(len(vals) for vals in cell_values) if cell_values else 0

            # Pad shorter lists and combine values
            for i in range(max_len):
                if len(cell_values) >= 1 and i < len(cell_values[0]):
                    all_years.append(cell_values[0][i])
                if len(cell_values) >= 2 and i < len(cell_values[1]):
                    all_numbers.append(cell_values[1][i])
                if len(cell_values) >= 3 and i < len(cell_values[2]):
                    all_percentages.append(cell_values[2][i])
                if len(cell_values) >= 4 and i < len(cell_values[3]):
                    all_rates.append(cell_values[3][i])

        # If we found data, create a DataFrame
        if all_years:
            # Make all lists the same length
            max_len = max(len(all_years), len(all_numbers), len(all_percentages), len(all_rates))

            # Pad shorter lists with None
            while len(all_years) < max_len:
                all_years.append(None)
            while len(all_numbers) < max_len:
                all_numbers.append(None)
            while len(all_percentages) < max_len:
                all_percentages.append(None)
            while len(all_rates) < max_len:
                all_rates.append(None)

            df = pd.DataFrame({
                "year": all_years,
                "pretrial_remand_number": all_numbers,
                "pretrial_remand_pct": all_percentages,
                "pretrial_remand_rate": all_rates,
            })

            # Clean the data - handle year formats like "2022/2023" by taking the latter year
            df["year"] = df["year"].astype(str).str.split("/").str[-1]
            # Handle 2-digit years (e.g., "2000/01" -> "01" should become 2001)
            df["year"] = df["year"].apply(lambda x: int(x) if len(str(x)) == 4 else (2000 + int(x) if int(x) <= 99 else int(x)))
            df["year"] = pd.to_numeric(df["year"], errors="coerce")

            # Clean numeric columns (remove commas, spaces, percentages)
            for col in ["pretrial_remand_number", "pretrial_remand_pct", "pretrial_remand_rate"]:
                if col in df.columns:
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.replace(" ", "", regex=False)
                        .str.replace("%", "", regex=False)
                        .str.replace("c.", "", regex=False)
                        .str.strip()
                    )
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df["country"] = country_name
            df = df.dropna(subset=["year"]).drop_duplicates()

            return df

    # Return empty DataFrame if no table found
    return pd.DataFrame()


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
        # First try to match 4-digit year
        match = re.search(r"(19|20)\d{2}", str(date_str))
        if match:
            return int(match.group(0))
        # Try to match 2-digit year (e.g., "31.12.23")
        match = re.search(r"\.(\d{2})(?:\s|$|\()", str(date_str))
        if match:
            year_2digit = int(match.group(1))
            # Convert 2-digit year to 4-digit (assuming 20xx for values <= current year % 100, else 19xx)
            return 2000 + year_2digit if year_2digit <= 99 else 1900 + year_2digit
        return None

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

    # Extract pre-trial/remand imprisonment table from the full page
    # These tables have a special format where multiple values are in one cell separated by <br/>
    pretrial_df = _extract_pretrial_table(soup, country_name)

    # Merge all data sources
    # Start with trend data or snapshot
    if not trend_df.empty:
        combined_df = pd.concat([trend_df, snapshot_df], ignore_index=True)
    else:
        combined_df = snapshot_df

    # Merge pre-trial data if available
    if not pretrial_df.empty and not combined_df.empty:
        # Merge on country and year
        combined_df = pd.merge(combined_df, pretrial_df, on=["country", "year"], how="outer")

    # Sort by country and year
    if "year" in combined_df.columns:
        combined_df = combined_df.sort_values(["country", "year"]).reset_index(drop=True)

    # Validate data
    _validate_data(combined_df, country_name)

    return combined_df
