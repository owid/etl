"""Load a snapshot and create a meadow dataset."""

import json
import re
import zipfile
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from owid.catalog.processing import read_from_df
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder, create_dataset

# Initialize log.
log = get_logger()


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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


def _extract_unit_from_soup(soup) -> Optional[str]:
    # Find the table in the soup.
    table = soup.find("table", class_="bodyTable")

    # Traverse backwards to find the paragraph or relevant tag immediately preceding the table.
    previous_element = table.find_previous()

    # Get the text of the first paragraph found.
    while previous_element:
        if previous_element.name == "p":
            unit_text = previous_element.get_text(strip=True)
            return unit_text if unit_text else None
        previous_element = previous_element.find_previous()

    return None


def _clean_raw_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Transform dataframe to have a column for country, commodity, year, note, value, and unit.
    # NOTE: The following is a bit convoluted, there may be an easier approach.
    df = df_raw.copy()
    df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns]
    df = df.melt(id_vars=["Category", "Country", "Commodity", "Sub-commodity", "Unit"])
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
            # NOTE: Ignore issues about imports/exports, which we decided to not use in the end.
            df_issues = df_issues[df_issues["Category"] == "Production"]
            if len(df_issues) > 0:
                log.warning(
                    f'Duplicated entries with different values on Category {set(df_issues["Category"])}, {len(df_issues["Country"].unique())} countries, Commodity {set(df_issues["Commodity"])}, and Sub-commodities: {set(df_issues["Sub-commodity"])}.'
                )
        # Drop duplicates.
        df = df.drop_duplicates(
            subset=["Category", "Country", "Commodity", "Sub-commodity", "Year", "note"], keep="last"
        ).reset_index(drop=True)
    df = df.pivot(
        index=["Category", "Country", "Commodity", "Sub-commodity", "Year", "Unit"], columns=["note"]
    ).reset_index()
    df.columns = [columns[0] if columns[0] != "value" else columns[1] for columns in df.columns]
    if "Note" not in df.columns:
        # If there were no notes in this table, there is no "Note" column in the pivoted table. Create one.
        df["Note"] = ""

    return df


def process_raw_data(data: Dict[str, Any]):
    # Initialize a dataframe that will contain all the combined data.
    df_all = pd.DataFrame(
        columns=["Country", "Commodity", "Sub-commodity", "Year", "Category", "Note", "Value", "Unit", "General notes"]
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

                # Extract the unit from the soup, if it's given.
                # It seems the unit simply appears on the top-right of the table, sometimes.
                # For example, this happens for mercury production:
                # https://www2.bgs.ac.uk/mineralsUK/statistics/wms.cfc?method=listResults&dataType=Production&commodity=95&dateFrom=2010&dateTo=2016&country=&agreeToTsAndCs=agreed
                # The unit randomly appears and disappears from one year to another.
                # See, e.g. Potash Chloride production, from 1984 to 1984:
                # https://www2.bgs.ac.uk/mineralsUK/statistics/wms.cfc?method=listResults&dataType=Production&commodity=118&dateFrom=1984&dateTo=1984&country=&agreeToTsAndCs=agreed
                # No units, whereas from 1985 to 1985:
                # https://www2.bgs.ac.uk/mineralsUK/statistics/wms.cfc?method=listResults&dataType=Production&commodity=118&dateFrom=1985&dateTo=1985&country=&agreeToTsAndCs=agreed
                # The unit appears.
                unit = _extract_unit_from_soup(soup=soup)

                # Create a raw dataframe with the data extracted from the soup.
                df_raw = _create_raw_dataframe_from_soup(soup=soup)

                # The field Sub-commodity is often empty.
                # Initially I thought this could be the total of a commodity, but it clearly isn't the case.
                # See, e.g. copper exports. The only case with empty subcommodity is South Africa in 1970 and 1971.
                # The value is ~106k tonnes of copper. This is smaller than "Unwrought" for the same country-years.
                # So it seems that the empty subcommodity is not the total.
                # Unfortunately, an empty subcommodity happens in around 38% of all rows.
                # Although we will see below that sometimes there are rows with different values for the same
                # country, commodity, sub-commodity, and year, which seems to be an issue in the dataset.
                df_raw.loc[df_raw["Sub-commodity"] == "", "Sub-commodity"] = "Unknown"

                # Add column for commodity.
                df_raw["Commodity"] = commodity

                # Add a column specifying the data type.
                df_raw["Category"] = data_type

                # Add a column specifying the unit.
                df_raw["Unit"] = unit

                # Process dataframe.
                df = _clean_raw_dataframe(df_raw=df_raw)

                # Map the row note symbol to its corresponding note text.
                notes_mapped = [
                    [footnotes.get(note, None) for note in str(notes).replace("(", "").replace(")", "")]
                    for notes in df["Note"].fillna("")
                ]
                if (None in sum(notes_mapped, [])) and (data_type == "Production"):
                    if (data_type == "Production") and (
                        commodity not in ["tantalum and niobium minerals", "ferro-alloys"]
                    ):
                        # NOTE: We choose here only "Production" because there are many issues for imports/exports.
                        #  We decided to not use imports/exports data.
                        # Also, some footnotes are missing for tantalum and niobium minerals and ferro-alloys, since their symbols have multiple letters, e.g. "ac", "ae" instead of just one (as in all other cases).
                        # Simply ignore these cases.
                        log.warning(f"Missing footnotes for: {data_type} - {commodity} - {year_start}")
                df["Note"] = [[note for note in notes if note] for notes in notes_mapped]

                # Add general notes as a new column.
                df["General notes"] = [[re.sub(r"^\d+:\s", "", note) for note in notes if note]] * len(df)

                # Add current dataframe to the combined dataframe.
                df_all = pd.concat([df_all, df], ignore_index=True)

    return df_all


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_mineral_statistics.zip")

    # Read compressed folder from snapshot.
    with zipfile.ZipFile(snap.path, "r") as _zipfile:
        # Read the JSON data inside the compressed folder.
        with _zipfile.open(f"{snap.metadata.short_name}.json") as json_file:
            json_data = json_file.read()

    #
    # Process data.
    #
    # Convert the JSON string into a dictionary
    data = json.loads(json_data)

    # Process raw data to create a combined dataframe.
    df_all = process_raw_data(data=data)

    # Create a table with the snapshot data and metadata.
    tb = read_from_df(data=df_all, metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["category", "country", "commodity", "sub_commodity", "year"])

    # To avoid ETL failing when storing the table, convert lists of notes to strings.
    tb["note"] = tb["note"].astype(str)
    tb["general_notes"] = tb["general_notes"].astype(str)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
