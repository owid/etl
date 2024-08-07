import json
from pathlib import Path
from typing import List, Optional, Union, cast

import pandas as pd
from owid import catalog
from owid.datautils.common import ExceptionFromDocstring, warn_on_list_of_entities
from owid.datautils.dataframes import map_series
from owid.datautils.io.json import load_json


def _load_countries_regions() -> pd.DataFrame:
    countries_regions = catalog.find("regions", namespace="regions").load()
    return cast(pd.DataFrame, countries_regions)


def _load_income_groups() -> pd.DataFrame:
    income_groups = catalog.find(table="income_groups_latest", namespace="wb", version="2024-03-11").load()
    return cast(pd.DataFrame, income_groups)


class RegionNotFound(ExceptionFromDocstring):
    """Region was not found in countries-regions dataset."""


def list_countries_in_region(
    region: str,
    countries_regions: Optional[pd.DataFrame] = None,
    income_groups: Optional[pd.DataFrame] = None,
) -> List[str]:
    """List countries that are members of a region.

    Parameters
    ----------
    region : str
        Name of the region (e.g. Europe).
    countries_regions : pd.DataFrame or None
        Countries-regions dataset, or None to load it from the catalog.
    income_groups : pd.DataFrame or None
        Income-groups dataset, or None, to load it from the catalog.

    Returns
    -------
    members : list
        Names of countries that are members of the region.

    """
    if countries_regions is None:
        countries_regions = _load_countries_regions()

    # TODO: Remove lines related to income_groups once they are included in countries-regions dataset.
    if income_groups is None:
        income_groups = _load_income_groups().reset_index()
    income_groups_names = income_groups["classification"].dropna().unique().tolist()  # type: ignore

    # TODO: Once countries-regions has additional columns 'is_historic' and 'is_country', select only countries, and not
    #  historical regions.
    if region in countries_regions["name"].tolist():
        # Find codes of member countries in this region.
        member_codes_str = countries_regions[countries_regions["name"] == region]["members"].item()
        if pd.isnull(member_codes_str):
            member_codes = []
        else:
            member_codes = json.loads(member_codes_str)
        # Get harmonized names of these countries.
        members = countries_regions.loc[member_codes]["name"].tolist()  # type: List[str]
    elif region in income_groups_names:
        members = income_groups[income_groups["classification"] == region]["country"].unique().tolist()  # type: ignore
    else:
        raise RegionNotFound

    return members
