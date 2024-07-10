"""Script to create a snapshot of dataset."""

import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io.json import load_json, save_json
from structlog import get_logger
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Initialize log.
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base url for data queries.
URL_BASE = "https://www2.bgs.ac.uk/mineralsuk/statistics/wms.cfc"

# Seconds to wait between consecutive queries.
TIME_BETWEEN_QUERIES = 0.1

# Define a temporary file while downloading the data, just in case the download fails. Remove the file afterwards.
TEMP_FILE = Path(__file__).parent / "temp.json"


def fetch_raw_data(data_types: List[str], years: List[int], commodity_to_id: Dict[str, int]):
    # Split years in groups of 9 years.
    years_start = [years[0]]
    for year in years[1:]:
        if year >= years_start[-1] + 9:
            years_start.append(year)

    # Calculate the total number of queries required.
    # n_queries = len(years_start) * len(commodity_to_id) * len(data_types)

    # Fetch raw data in as many queries as possible.
    data = {}
    for data_type in tqdm(data_types, desc="Data type"):
        data[data_type] = {}
        for commodity in tqdm(commodity_to_id, desc="Commodity"):
            data[data_type][commodity] = {}
            commodity_id = commodity_to_id[commodity]
            for year_start in years_start:
                year_end = year_start + 9

                # Construct the url for the query.
                query_url = (
                    URL_BASE
                    + f"?method=listResults&dataType={data_type}&commodity={commodity_id}&dateFrom={year_start}&dateTo={year_end}&country=&agreeToTsAndCs=agreed"
                )

                # Get response.
                response = requests.get(query_url)

                # Store content.
                data[data_type][commodity][year_start] = response.content.decode("utf-8")

                # Wait before sending next query.
                time.sleep(TIME_BETWEEN_QUERIES)

    return data


def _create_raw_dataframe_from_soup(soup: BeautifulSoup) -> pd.DataFrame:
    # Locate the table.
    table = soup.find("table", class_="bodyTable")

    # Extract headers with multi-level structure.
    headers = []
    sub_headers = []
    for th in table.find_all("th"):  # type: ignore
        colspan = th.get("colspan")
        if colspan:
            headers.extend([th.text.strip()] * int(colspan))
            sub_headers.extend(["Note", "Value"] * (int(colspan) // 2))
        else:
            headers.append(th.text.strip())
            sub_headers.append("")

    # Extract rows (skipping the header row).
    rows = []
    for tr in table.find_all("tr")[1:]:  # type: ignore
        row = []
        for td in tr.find_all("td"):
            row.append(td.text.strip())
        rows.append(row)

    # Combine the headers into a MultiIndex.
    multi_headers = pd.MultiIndex.from_arrays([headers, sub_headers])

    # If some years between year_start and year_end are not informed, remove them.
    _n_rows = list(set([len(row) for row in rows]))
    assert len(_n_rows) == 1, "Unknown error when extracting rows of data."
    multi_headers = multi_headers[0 : _n_rows[0]]

    # Create a multiindex DataFrame.
    df_raw = pd.DataFrame(rows, columns=multi_headers)

    return df_raw


def _extract_notes_and_footnotes_from_soup(soup: BeautifulSoup) -> Tuple[List[str], Dict[str, str]]:
    # Extract notes and footnotes from the soup.
    notes_section = soup.find("h2", string="Notes")

    # Extract notes and footnotes separately.
    notes = []
    footnotes = []
    if notes_section:
        notes_heading = notes_section.find_next("h3", string="Table notes")
        if notes_heading:
            for sibling in notes_heading.find_next_siblings():
                if sibling.name == "h3" and sibling.text == "Footnotes":  # type: ignore
                    break
                if sibling.name == "p":  # type: ignore
                    notes.append(sibling.text.strip())

        footnotes_heading = notes_section.find_next("h3", string="Footnotes")
        if footnotes_heading:
            for sibling in footnotes_heading.find_next_siblings():
                if sibling.name == "h2" and sibling.text == "Export data":  # type: ignore
                    break
                if sibling.name == "p":  # type: ignore
                    footnotes.append(sibling.text.strip())

    # Convert footnotes into a dictionary, mapping the mark of the footnote to its corresponding text.
    footnotes = {f"{note.split(')', 1)[0].strip()}": note.split(")", 1)[1].strip() for note in footnotes}
    footnotes[""] = ""

    return notes, footnotes


def _clean_raw_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Transform dataframe to have a column for country, commodity, year, note and value.
    # NOTE: The following is a bit convoluted, there may be an easier approach.
    df = df_raw.copy()
    df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns]
    df = df.melt(id_vars=["Category", "Country", "Commodity", "Sub-commodity"])
    df["Year"] = df["variable"].str.split("_").str[0]
    df["note"] = df["variable"].str.split("_").str[1]
    df = df.drop(columns="variable")
    # Drop rows that have no values.
    df = df[df["value"] != ""].reset_index(drop=True)
    # There are separate rows for the same country and sub-commodity.
    # For example, Imports of cobalt in Australia, Oxides, 1988 appears in one row, and all other years appear in a different row.
    # Check if there are multiple rows for the same country-commodity-year-note (we include note, which can be either "Value" or "Note").
    df_repeated = df[
        df.duplicated(subset=["Category", "Country", "Commodity", "Sub-commodity", "Year", "note"], keep=False)
    ]
    if not df_repeated.empty:
        # Check that the repeated rows have the exact same data values, in which case, simply drop them.
        df_repeated_with_same_values = df_repeated[
            df_repeated.duplicated(
                subset=["Category", "Country", "Commodity", "Sub-commodity", "Year", "note", "value"], keep=False
            )
        ]
        index_issues = sorted(set(df_repeated.index) - set(df_repeated_with_same_values.index))
        if not df_repeated.equals(df_repeated_with_same_values):
            df_issues = df.loc[index_issues]
            log.warning(
                f'Duplicated entries with different values on Category {set(df_issues["Category"])}, {len(df_issues["Country"].unique())} countries, Commodity {set(df_issues["Commodity"])}, and Sub-commodities: {set(df_issues["Sub-commodity"])}.'
            )
        # Drop duplicates.
        df = df.drop_duplicates(
            subset=["Category", "Country", "Commodity", "Sub-commodity", "Year", "note"], keep="last"
        ).reset_index(drop=True)
    df = df.pivot(index=["Category", "Country", "Commodity", "Sub-commodity", "Year"], columns=["note"]).reset_index()
    df.columns = [columns[0] if columns[0] != "value" else columns[1] for columns in df.columns]
    if "Note" not in df.columns:
        # If there were no notes in this table, there is no "Note" column in the pivoted table. Create one.
        df["Note"] = ""

    return df


def process_raw_data(data: Dict[str, Any]):
    # Initialize a dataframe that will contain all the combined data.
    df_all = pd.DataFrame(
        columns=["Country", "Commodity", "Sub-commodity", "Year", "Category", "Note", "Value", "General notes"]
    )
    # Go through all nests in the fetched data, process and add to the combined dataframe.
    for data_type in tqdm(list(data), desc="Data type"):
        for commodity in tqdm(list(data[data_type]), desc="Commodity"):
            for year_start in map(int, list(data[data_type][commodity])):
                # log.info(f"Processing {data_type} - {commodity} - {year_start}")

                # Get the content for this iteration.
                content = data[data_type][commodity][str(year_start)]

                # Initialize a soup with this content.
                soup = BeautifulSoup(content, "html.parser")

                if "No results" in soup.text:
                    # log.info(f"No results for {data_type} - {commodity} - {year_start}")
                    continue

                # Extract notes and footnotes from the soup.
                notes, footnotes = _extract_notes_and_footnotes_from_soup(soup=soup)

                # Create a raw dataframe with the data extracted from the soup.
                df_raw = _create_raw_dataframe_from_soup(soup=soup)

                # The field Sub-commodity is often empty.
                # I assume that this is the total of that commodity.
                # Although we will see below that sometimes there are rows with different values for the same
                # country, commodity, sub-commodity, and year, which seems to be an issue in the dataset.
                df_raw.loc[df_raw["Sub-commodity"] == "", "Sub-commodity"] = "Total"

                # Add column for commodity.
                df_raw["Commodity"] = commodity

                # Add a column specifying the data type.
                df_raw["Category"] = data_type

                # Process dataframe.
                df = _clean_raw_dataframe(df_raw=df_raw)

                # Map the row note symbol to its corresponding note text.
                notes_mapped = [
                    [footnotes.get(note, None) for note in str(notes).replace("(", "").replace(")", "")]
                    for notes in df["Note"].fillna("")
                ]
                if None in sum(notes_mapped, []):
                    log.warning(f"Missing footnotes for: {data_type} - {commodity} - {year_start}")
                df["Note"] = [
                    ". ".join(
                        [
                            footnotes[note] if note in footnotes else ""
                            for note in str(notes).replace("(", "").replace(")", "")
                        ]
                    ).replace("..", ".")
                    for notes in df["Note"].fillna("")
                ]

                # Add general notes as a new column.
                df["General notes"] = ". ".join(notes).replace("..", ".")

                # Add current dataframe to the combined dataframe.
                df_all = pd.concat([df_all, df], ignore_index=True)

    return df_all


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"bgs/{SNAPSHOT_VERSION}/world_mineral_statistics.csv")

    # Load the download data page, to be able to extract the mappings of the different elements.
    page_map = requests.get(URL_BASE + "?method=searchWMS")
    soup = BeautifulSoup(page_map.text, "html.parser")

    # Map mineral names (commodities) to ids.
    soup_commodity = soup.find("select", id="commodity").find_all("option")  # type: ignore
    commodity_to_id = {option.text.strip(): int(option["value"]) for option in soup_commodity if option["value"]}

    # Data type options are simply "Imports", "Exports" and "Production".
    data_types = ["Imports", "Exports", "Production"]

    # Get the list of available years.
    soup_years = soup.find("select", id="dateFrom").find_all("option")  # type: ignore
    years = [int(option.text.strip()) for option in soup_years if option["value"]]

    # For countries we will simply download all (for which we can set "country=" in the query).
    # Queries need to be in groups of maximum 10 years.
    # Example query:
    # https://www2.bgs.ac.uk/mineralsUK/statistics/wms.cfc?method=listResults&dataType=Imports&commodity=29&dateFrom=1970&dateTo=1979&country=&agreeToTsAndCs=agreed

    if TEMP_FILE.exists():
        log.warning(f"Loading data from temporary file: {TEMP_FILE}")
        data = load_json(TEMP_FILE)
    else:
        # First fetch all necessary data, without processing it.
        data = fetch_raw_data(data_types=data_types, years=years, commodity_to_id=commodity_to_id)
        # Save data to a temporary json file, in case something fails in the processing.
        save_json(data=data, json_file=TEMP_FILE)

    # Process raw data to create a combined dataframe.
    df_all = process_raw_data(data=data)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df_all)

    # Now that the processing has been successful, remove the temporary file.
    if TEMP_FILE.exists():
        TEMP_FILE.unlink()


if __name__ == "__main__":
    main()
