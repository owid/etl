#
#  internal.py
#
#  Internal APIs subject to change at any time.
#

import io
import json
from dataclasses import dataclass

import pandas as pd
import requests


class LicenseError(Exception):
    pass


class ChartNotFoundError(Exception):
    pass


@dataclass
class _GrapherBundle:
    slug: str
    config: dict
    metadata: dict
    _df: pd.DataFrame

    def to_frame(self) -> pd.DataFrame:
        df = self._df.copy()

        # Rename columns to match expected format
        df = df.rename(columns={"Entity": "entities", "Year": "years", "Day": "years"})

        # Drop the Code column (not used in the old API)
        if "Code" in df.columns:
            df = df.drop(columns=["Code"])

        # Attach metadata to the DataFrame
        df.attrs["slug"] = self.slug
        df.attrs["url"] = f"https://ourworldindata.org/grapher/{self.slug}"

        # Build per-column metadata from the metadata.json response
        columns_meta = self.metadata.get("columns", {})
        df.attrs["metadata"] = {}
        for col_name, col_meta in columns_meta.items():
            short_name = col_meta.get("shortName", col_name)
            if short_name in df.columns:
                df.attrs["metadata"][short_name] = col_meta

        # If there's only one data column, rename it to the slug-based name
        value_cols = [c for c in df.columns if c not in ("entities", "years")]
        if len(value_cols) == 1:
            old_name = value_cols[0]
            new_name = self.slug.replace("-", "_")
            df = df.rename(columns={old_name: new_name})
            if old_name in df.attrs["metadata"]:
                df.attrs["metadata"][new_name] = df.attrs["metadata"].pop(old_name)
            df.attrs["value_col"] = new_name

        # Rename "years" to "dates" if the values are date strings
        if "years" in df.columns and df["years"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").all():
            df = df.rename(columns={"years": "dates"})

        return df

    def __repr__(self):
        return f"GrapherBundle(slug={self.slug}, config=..., metadata=...)"


def _fetch_bundle(slug: str) -> _GrapherBundle:
    base_url = f"https://ourworldindata.org/grapher/{slug}"

    # Fetch CSV data
    csv_resp = requests.get(f"{base_url}.csv?useColumnShortNames=true")
    if csv_resp.status_code == 404:
        raise ChartNotFoundError(f"No such chart found at {base_url}")
    if csv_resp.status_code == 403:
        # Non-redistributable data
        try:
            error_data = csv_resp.json()
            raise LicenseError(error_data.get("error", "This chart contains non-redistributable data"))
        except (json.JSONDecodeError, ValueError):
            raise LicenseError("This chart contains non-redistributable data that cannot be downloaded")
    csv_resp.raise_for_status()

    df = pd.read_csv(io.StringIO(csv_resp.text))

    # Fetch metadata
    meta_resp = requests.get(f"{base_url}.metadata.json")
    meta_resp.raise_for_status()
    metadata = meta_resp.json()

    # Fetch config
    config_resp = requests.get(f"{base_url}.config.json")
    config_resp.raise_for_status()
    config = config_resp.json()

    return _GrapherBundle(slug=slug, config=config, metadata=metadata, _df=df)
