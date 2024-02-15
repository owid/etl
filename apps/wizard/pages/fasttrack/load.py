"""Functions and variables to load data from CSV and Google Sheets.

Relies on Streamlit to print messages.
"""
import concurrent.futures
import datetime as dt
import json
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from owid.catalog import (
    DatasetMeta,
    License,
    Origin,
    Source,
    VariableMeta,
    VariablePresentationMeta,
)
from owid.catalog.utils import underscore

from apps.wizard.pages.fasttrack.utils import _decrypt
from etl.grapher_helpers import INT_TYPES
from etl.paths import SNAPSHOTS_DIR
from etl.snapshot import SnapshotMeta

# these IDs are taken from template sheet, they will be different if someone
# creates a new sheet from scratch and use those names
SHEET_TO_GID = {
    "data": 409110122,
    "raw_data": 901452831,
    "variables_meta": 777328216,
    "dataset_meta": 1719161864,
    "sources_meta": 1399503534,
    "origins_meta": 279169148,
}


def load_existing_sheets_from_snapshots() -> List[Dict[str, str]]:
    """Load available sheets from local environment."""
    # get all fasttrack snapshots
    metas = [SnapshotMeta.load_from_yaml(path) for path in (SNAPSHOTS_DIR / "fasttrack").rglob("*.dvc")]

    existing_sheets = []
    for meta in metas:
        # exclude local CSVs
        if (getattr(meta.source, "name", None) or getattr(meta.origin, "version_producer")) == "Local CSV":
            continue

        if meta.source:
            assert meta.source.source_data_url
            url = meta.source.source_data_url
            date_accessed = meta.source.date_accessed
        elif meta.origin:
            assert meta.origin.url_download
            url = meta.origin.url_download
            date_accessed = meta.origin.date_accessed
        else:
            raise ValueError("Neither source nor origin")

        # decrypt URLs if private
        if not meta.is_public:
            url = _decrypt(url)

        existing_sheets.append(
            {
                "label": f"{meta.name} / {meta.version}",
                "value": url,
                "is_public": meta.is_public,
                "date_accessed": str(date_accessed),
            }
        )

    # sort them by date accessed
    existing_sheets.sort(key=lambda m: m["date_accessed"], reverse=True)  # type: ignore

    return existing_sheets


@st.cache_data(show_spinner=False)
def load_data_from_csv(uploaded_file):
    print("RUNNING FOR SHEETS")
    # Read CSV file as a dataframe
    st.write("Importing CSV...")
    csv_df = pd.read_csv(uploaded_file)

    # Parse dataframe
    st.write("Parsing data...")
    data = parse_data_from_csv(csv_df)

    # Obtain dataset and other objects
    st.write("Parsing metadata...")
    dataset_meta, variables_meta_dict, origin = parse_metadata_from_csv(
        uploaded_file.name,
        csv_df.columns,
    )

    # Success message
    st.success("Data imported from CSV")

    return data, dataset_meta, variables_meta_dict, origin


@st.cache_data(show_spinner=False)
def load_data_from_sheets(sheets_url, _status):
    """First step to load data."""
    # Show status progress as we import data
    st.info(
        """
        Note that Google Sheets refreshes its published version every 5 minutes, so you may need to wait a bit after you update your data.
        """
    )
    # Sanity check
    st.write("Sanity checks...")
    if "?output=csv" not in sheets_url:
        st.exception(
            ValidationError(
                f"URL does not contain `?output=csv`. Have you published it as CSV and not as HTML by accident? URL was {sheets_url}"
            )
        )
        _status.update(state="error")
        st.stop()
    else:
        try:
            # Import data from Google Sheets
            st.write(f"Importing [sheet]({sheets_url.replace('?output=csv', '')})...")
            google_sheets = import_google_sheets(sheets_url)
            # TODO: it would make sense to repeat the import until we're sure that it has been updated
            # we wouldn't risk importing data that is not up to date then
            # the question is how much can we trust the timestamp in the published version

            # Parse data into dataframe
            st.write("Parsing data...")
            data = parse_data_from_sheets(google_sheets["data"])

            # Obtain dataset and other objects
            st.write("Creating dataset...")
            dataset_meta, variables_meta_dict, origin = parse_metadata_from_sheets(
                google_sheets["dataset_meta"],
                google_sheets["variables_meta"],
                google_sheets["sources_meta"],
                google_sheets["origins_meta"],
            )

        except urllib.error.HTTPError:
            st.exception(
                ValidationError(
                    "Sheet not found, have you copied the template? Creating new Google Sheets document or new "
                    "sheets with the same name in the existing document does not work."
                )
            )
            _status.update(state="error")
            st.stop()
        except ValidationError as e:
            st.exception(e)
            st.stop()
        else:
            st.success(
                f"Data imported (sheet refreshed {_last_updated_before_minutes(google_sheets['dataset_meta'])} minutes ago)"
            )

    return data, dataset_meta, variables_meta_dict, origin


###################################
## CSV ############################
###################################
def parse_data_from_csv(csv_df: pd.DataFrame) -> pd.DataFrame:
    data = parse_data_from_sheets(csv_df)

    # underscore columns from CSV
    data.columns = [underscore(col) for col in data.columns]

    return data


def parse_metadata_from_csv(
    filename: str, columns: List[str]
) -> Tuple[DatasetMeta, Dict[str, VariableMeta], Optional[Origin]]:
    filename = filename.replace(".csv", "")
    title = f"DRAFT {filename}"
    dataset_dict: dict[str, Any] = {
        "title": title,
        "short_name": underscore(filename),
        "version": "latest",
        "namespace": "fasttrack",
        "description": "",
    }

    variables_dict = {
        underscore(col): {
            "title": col,
            "unit": "",
        }
        for col in columns
        if col.lower() not in ("country", "year", "entity")
    }

    dataset_dict["sources"] = [
        dict(
            name="Unknown",
            url="",
            publication_year=None,
        )
    ]
    dataset_dict["licenses"] = [
        {
            "url": None,
            "name": None,
        }
    ]

    return DatasetMeta(**dataset_dict), {k: VariableMeta(**v) for k, v in variables_dict.items()}, None


###################################
## SHEETS
###################################
class ValidationError(Exception):
    pass


def _fetch_url_or_empty_dataframe(url, **kwargs):
    try:
        return pd.read_csv(url, **kwargs)
    except urllib.error.HTTPError:
        return pd.DataFrame()


def import_google_sheets(url: str) -> Dict[str, Any]:
    # read dataset first to check if we're using data_url instead of data sheet
    dataset_meta = pd.read_csv(f"{url}&gid={SHEET_TO_GID['dataset_meta']}", header=None)
    data_url = _get_data_url(dataset_meta, url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        data_future = executor.submit(lambda x: pd.read_csv(x), data_url)
        variables_meta_future = executor.submit(lambda x: pd.read_csv(x), f"{url}&gid={SHEET_TO_GID['variables_meta']}")
        sources_meta_future = executor.submit(
            lambda url: _fetch_url_or_empty_dataframe(url, header=None), f"{url}&gid={SHEET_TO_GID['sources_meta']}"
        )
        origins_meta_future = executor.submit(
            lambda url: _fetch_url_or_empty_dataframe(url, header=None), f"{url}&gid={SHEET_TO_GID['origins_meta']}"
        )

    return {
        "data": data_future.result(),
        "variables_meta": variables_meta_future.result(),
        "dataset_meta": dataset_meta,
        "sources_meta": sources_meta_future.result(),
        "origins_meta": origins_meta_future.result(),
    }


def parse_data_from_sheets(data_df: pd.DataFrame) -> pd.DataFrame:
    # lowercase columns names
    for col in data_df.columns:
        if col.lower() in ("entity", "year", "country"):
            data_df.rename(columns={col: col.lower()}, inplace=True)

    if "entity" in data_df.columns:
        data_df = data_df.rename(columns={"entity": "country"})

    if "year" not in data_df.columns:
        raise ValidationError("Missing column 'year' in data")

    if "country" not in data_df.columns:
        raise ValidationError("Missing column 'country' in data")

    # check types
    if data_df.year.dtype not in INT_TYPES:
        raise ValidationError("Column 'year' should be integer")

    return data_df.set_index(["country", "year"])


def _parse_sources(sources_meta_df: pd.DataFrame) -> Optional[Source]:
    if sources_meta_df.empty:
        return None

    sources = sources_meta_df.set_index(0).T.to_dict(orient="records")

    if not sources:
        return None

    assert len(sources) == 1, "Only one source is supported for now"
    source = sources[0]

    if pd.isnull(source.get("date_accessed")):
        source.pop("date_accessed")

    if pd.isnull(source.get("publication_year")):
        source.pop("publication_year")

    # publisher_source is not used anymore
    source.pop("publisher_source", None)
    # short_name is not used anymore
    source.pop("short_name", None)

    return Source(**source)


def _parse_origins(origins_meta_df: pd.DataFrame) -> Optional[Origin]:
    if origins_meta_df.empty:
        return None

    origins = origins_meta_df.set_index(0).T.to_dict(orient="records")

    if not origins:
        return None

    assert len(origins) == 1, "Only one source is supported for now"
    origin = origins[0]

    origin = _prune_empty(origin)  # type: ignore

    # parse license fields
    if origin.get("license.name") or origin.get("license.url"):
        origin["license"] = License(name=origin.pop("license.name", None), url=origin.pop("license.url", None))

    return Origin(**origin)


def _parse_dataset(dataset_meta_df: pd.DataFrame) -> DatasetMeta:
    dataset_dict = _prune_empty(dataset_meta_df.set_index(0)[1].to_dict())  # type: ignore
    dataset_dict["namespace"] = "fasttrack"  # or should it be owid? or institution specific?
    dataset_dict.pop("updated")
    dataset_dict.pop("external_csv", None)
    dataset_dict.setdefault("description", "")

    try:
        if dataset_dict["version"] != "latest":
            dt.datetime.strptime(dataset_dict["version"], "%Y-%m-%d")
    except ValueError:
        raise ValidationError(f"Version `{dataset_dict['version']}` is not in YYYY-MM-DD format")

    # manadatory dataset fields
    for key in ("title", "short_name", "version"):
        if key not in dataset_dict:
            raise ValidationError(f"Missing mandatory field '{key}' from sheet 'dataset_meta'")

    # deprecated field
    dataset_dict.pop("sources", None)

    license = License(name=dataset_dict.pop("license_name", None), url=dataset_dict.pop("license_url", None))

    dataset_meta = DatasetMeta(**dataset_dict)
    dataset_meta.licenses = [license]

    return dataset_meta


def _parse_variables(variables_meta_df: pd.DataFrame) -> Dict[str, VariableMeta]:
    variables_meta_df = variables_meta_df.dropna(subset=["short_name"])

    variables_list = [_prune_empty(v) for v in variables_meta_df.to_dict(orient="records")]  # type: ignore

    # default variable values
    for variable in variables_list:
        variable.setdefault("unit", "")

    # move display.* columns to display object
    for variable in variables_list:
        for k in list(variable.keys()):
            if k.startswith("display."):
                variable.setdefault("display", {})[k[8:]] = variable.pop(k)

    out = {}
    for variable in variables_list:
        # sources field is deprecated
        variable.pop("sources", None)
        short_name = variable.pop("short_name")

        if variable.get("presentation"):
            variable["presentation"] = VariablePresentationMeta(**json.loads(variable["presentation"]))

        var_meta = VariableMeta(**variable)

        out[short_name] = var_meta

    return out


def parse_metadata_from_sheets(
    dataset_meta_df: pd.DataFrame,
    variables_meta_df: pd.DataFrame,
    sources_meta_df: pd.DataFrame,
    origins_meta_df: pd.DataFrame,
) -> Tuple[DatasetMeta, Dict[str, VariableMeta], Optional[Origin]]:
    source = _parse_sources(sources_meta_df)
    origin = _parse_origins(origins_meta_df)
    dataset_meta = _parse_dataset(dataset_meta_df)
    variables_meta_dict = _parse_variables(variables_meta_df)

    if origin and source:
        raise ValidationError("Using origins and sources together is not yet supported")

    # put all sources and origins to dataset level
    if source:
        dataset_meta.sources = [source]

    return dataset_meta, variables_meta_dict, origin


def _prune_empty(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None and v != "" and not pd.isnull(v)}


def _get_data_url(dataset_meta: pd.DataFrame, url: str) -> str:
    """Get data url from dataset_meta field or use data sheet if dataset_meta is empty."""
    data_url = dataset_meta.set_index(0)[1].to_dict().get("external_csv")

    if data_url and not pd.isnull(data_url):
        # files on Google Drive need modified link for downloading raw csv
        if "drive.google.com" in data_url:
            data_url = data_url.replace("file/d/", "uc?id=").replace("/view?usp=share_link", "&export=download")
    else:
        # use data sheet
        data_url = f"{url}&gid={SHEET_TO_GID['data']}"

    return data_url


def _last_updated_before_minutes(dataset_meta: pd.DataFrame) -> int:
    updated = dataset_meta.set_index(0)[1].loc["updated"]
    if pd.isnull(updated):
        return 0
    else:
        td = pd.Timestamp.utcnow() - pd.to_datetime(
            dataset_meta.set_index(0)[1].loc["updated"], dayfirst=True, utc=True
        )
        return int(td.total_seconds() / 60)
