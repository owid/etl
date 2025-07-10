"""Helper functions for reading data from Notion."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from structlog import get_logger

from etl.config import NOTION_API_KEY, NOTION_DATA_PROVIDERS_CONTACTS_TABLE_URL, NOTION_IMPACT_HIGHLIGHTS_TABLE_URL

# Initialize log.
log = get_logger()

# Notion API URL.
NOTION_API_URL = "https://api.notion.com/v1"
# To check for the latest Notion API version:
# https://developers.notion.com/reference/versioning
NOTION_API_VERSION = "2022-06-28"

# Arbitrary first date to include in the Notion impact highlights table.
NOTION_IMPACT_HIGHLIGHTS_MIN_DATE = "2000-01-01"
# Maximum date to consider for impact highlights (today).
NOTION_IMPACT_HIGHLIGHTS_MAX_DATE = str(datetime.today().date())


class NotionClient:
    """Client for interacting with Notion API."""

    def __init__(self):
        """
        Initialize Notion client.

        """
        self.headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_API_VERSION,
        }

    def get_database(self, database_id: str, page_size: int = 100, max_rows: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch all data from a Notion database.

        Parameters
        ----------
        database_id : str
            ID of the Notion database.
        page_size : int
            Number of results per page (max 100).
        max_rows : int, optional
            Maximum number of rows to fetch. If None, fetch all rows.

        Returns
        -------
        database_results : dict
            Database results.
        """
        url = f"{NOTION_API_URL}/databases/{database_id}/query"

        all_results = []
        has_more = True
        start_cursor = None

        while has_more and (max_rows is None or len(all_results) < max_rows):
            current_page_size = page_size
            if max_rows is not None:
                remaining_rows = max_rows - len(all_results)
                current_page_size = min(page_size, remaining_rows)

            payload = {"page_size": current_page_size}
            if start_cursor:
                payload["start_cursor"] = start_cursor

            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()

            data = response.json()
            fetched_results = data["results"]

            # If max_rows is set, limit the results to not exceed max_rows.
            if max_rows is not None:
                remaining_rows = max_rows - len(all_results)
                fetched_results = fetched_results[:remaining_rows]

            all_results.extend(fetched_results)

            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")

            log.info(f"Fetched {len(fetched_results)} rows from Notion, total fetched: {len(all_results)}")

            # Stop if we have reached max_rows.
            if max_rows is not None and len(all_results) >= max_rows:
                break

        database_results = {"results": all_results}

        return database_results

    def _parse_property(self, prop_data: Dict[str, Any]) -> Any:
        """Parse a Notion property based on its type."""
        prop_type = prop_data["type"]

        if prop_type == "title":
            return prop_data["title"][0]["plain_text"] if prop_data["title"] else ""
        elif prop_type == "rich_text":
            return prop_data["rich_text"][0]["plain_text"] if prop_data["rich_text"] else ""
        elif prop_type == "number":
            return prop_data["number"]
        elif prop_type == "select":
            return prop_data["select"]["name"] if prop_data["select"] else ""
        elif prop_type == "multi_select":
            return [item["name"] for item in prop_data["multi_select"]]
        elif prop_type == "date":
            return prop_data["date"]["start"] if prop_data["date"] else ""
        elif prop_type == "checkbox":
            return prop_data["checkbox"]
        elif prop_type == "url":
            return prop_data["url"]
        elif prop_type == "email":
            return prop_data["email"]
        elif prop_type == "phone_number":
            return prop_data["phone_number"]
        elif prop_type == "people":
            return [person["name"] for person in prop_data["people"]]
        elif prop_type == "relation":
            return [rel["id"] for rel in prop_data["relation"]]
        elif prop_type == "rollup":
            rollup_type = prop_data["rollup"]["type"]
            if rollup_type == "number":
                return prop_data["rollup"]["number"]
            elif rollup_type == "array":
                # Just return count for simplicity.
                return len(prop_data["rollup"]["array"])
            else:
                return str(prop_data["rollup"])
        elif prop_type == "formula":
            formula_type = prop_data["formula"]["type"]
            if formula_type in ["string", "number", "boolean"]:
                return prop_data["formula"][formula_type]
            else:
                return str(prop_data["formula"])
        else:
            log.warning(f"Unsupported property type: {prop_type}")
            return str(prop_data)

    def database_to_dataframe(
        self, database_id: str, include_row_urls: bool = True, max_rows: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Convert a Notion database to a pandas DataFrame.

        Parameters
        ----------
        database_id : str
            The ID of the Notion database.
        include_row_urls : bool
            Whether to include a column with URLs to each row.
        max_rows : int, optional
            Maximum number of rows to fetch. If None, fetch all rows.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the database data.
        """
        data = self.get_database(database_id, max_rows=max_rows)

        rows = []
        for page in data["results"]:
            row = {}

            # Add the row URL if requested.
            if include_row_urls:
                page_id = page["id"].replace("-", "")
                row["notion_url"] = f"https://www.notion.so/{page_id}"

            # Parse all properties.
            for prop_name, prop_data in page["properties"].items():
                row[prop_name] = self._parse_property(prop_data)

            rows.append(row)

        df = pd.DataFrame(rows)
        log.info(f"Loaded {len(df)} rows from Notion database {database_id}")

        return df

    @staticmethod
    def get_database_id_from_url(notion_url: str) -> str:
        """Extract database ID from a Notion URL."""
        database_id = notion_url.split("?")[0].split("/")[-1]

        return "".join(c for c in database_id if c.isalnum())


def get_table_from_notion_url(
    notion_url: str, include_row_urls: bool = False, max_rows: Optional[int] = None
) -> pd.DataFrame:
    """Create a dataframe from a table in a Notion page."""
    client = NotionClient()
    database_id = client.get_database_id_from_url(notion_url=notion_url)
    df = client.database_to_dataframe(database_id=database_id, include_row_urls=include_row_urls, max_rows=max_rows)

    return df


def get_impact_highlights(
    producers: Optional[List[str]] = None,
    min_date: str = NOTION_IMPACT_HIGHLIGHTS_MIN_DATE,
    max_date: str = NOTION_IMPACT_HIGHLIGHTS_MAX_DATE,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    # Name of column of related data producers.
    producer_col = "Data provider(s) related"
    # Name of column containing the date.
    date_col = "Date"

    # Fetch impact highlights from Notion.
    df = get_table_from_notion_url(
        notion_url=NOTION_IMPACT_HIGHLIGHTS_TABLE_URL,  # type: ignore
        include_row_urls=True,
        max_rows=max_rows,
    )

    # Filter by date.
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[(df[date_col] >= pd.to_datetime(min_date)) & (df[date_col] <= pd.to_datetime(max_date))].reset_index(
        drop=True
    )

    log.info(f"Filtered impact highlights to date range {min_date} to {max_date}, {len(df)} rows remaining")

    if producers is not None:
        # Find indexes of rows where the given producers are mentioned.
        indexes = [i for i, producers_in_row in enumerate(df[producer_col]) if set(producers) & set(producers_in_row)]
        # Select relevant rows.
        df = df.loc[indexes].reset_index(drop=True)

    return df


def get_data_producer_contacts(producers: Optional[List[str]] = None) -> pd.DataFrame:
    # Fetch data providers contacts table from Notion.
    df = get_table_from_notion_url(notion_url=NOTION_DATA_PROVIDERS_CONTACTS_TABLE_URL)  # type: ignore

    if producers is not None:
        # Select relevant producers.
        df = df[df["Name"].isin(producers)].reset_index(drop=True)

    return df
