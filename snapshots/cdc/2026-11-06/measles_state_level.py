"""Script to create a snapshot of dataset.

Reported measles cases (total, imported and indigenous) by year and reporting area, from the CDC WONDER
NNDSS Annual Summary Data (https://wonder.cdc.gov/nndss-annual-summary.html).

We re-run the saved WONDER query D422F203 through the WONDER XML API and store the result table as it comes.
The API only accepts a full set of request parameters, so we read them from the saved query's request form.
"""

import io
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from xml.sax.saxutils import escape

import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
URL_SAVED_QUERY = "https://wonder.cdc.gov/controller/saved/D130/D422F203"
URL_API = "https://wonder.cdc.gov/controller/datarequest/D130"
COLUMNS = ["Disease", "Year", "Regions/States", "Case Count"]


def get_request_parameters() -> dict[str, list[str]]:
    """Read the request parameters of the saved query from its request form."""
    response = requests.get(URL_SAVED_QUERY)
    response.raise_for_status()
    form = BeautifulSoup(response.content, "html.parser").find("form", id="wonderform")
    assert form is not None, "Could not find the request form of the saved WONDER query."

    params = defaultdict(list)
    for el in form.find_all(["input", "select", "textarea"]):
        name = el.get("name")
        if not name:
            continue
        if el.name == "input":
            kind = (el.get("type") or "text").lower()
            if kind in ("submit", "button", "image", "reset"):
                continue
            if kind in ("checkbox", "radio") and not el.has_attr("checked"):
                continue
            params[name].append(el.get("value", ""))
        elif el.name == "select":
            selected = [o.get("value", "") for o in el.find_all("option") if o.has_attr("selected")]
            if not selected and not el.has_attr("multiple"):
                first = el.find("option")
                selected = [first.get("value", "")] if first else []
            params[name].extend(selected)
        else:
            params[name].append(el.text)
    return params


def download_data() -> pd.DataFrame:
    params = get_request_parameters()
    request_xml = (
        "<request-parameters>"
        + "".join(
            f"<parameter><name>{escape(name)}</name>"
            + "".join(f"<value>{escape(value)}</value>" for value in values)
            + "</parameter>"
            for name, values in params.items()
        )
        + "</request-parameters>"
    )
    response = requests.post(URL_API, data={"request_xml": request_xml, "accept_datause_restrictions": "true"})
    response.raise_for_status()

    # Each row of the result table holds the disease, year and reporting area labels, followed by the case count.
    root = ET.parse(io.BytesIO(response.content)).getroot()
    rows = []
    for row in root.iter("r"):
        cells = [cell.get("l", cell.get("v")) for cell in row.findall("c")]
        assert len(cells) == len(COLUMNS), f"Unexpected row in WONDER response: {cells}"
        rows.append(dict(zip(COLUMNS, cells)))
    assert rows, "WONDER returned no data."
    return pd.DataFrame(rows)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/measles_state_level.csv")

    # Download data, add file to DVC and upload to S3.
    snap.create_snapshot(data=download_data(), upload=upload)
