"""Cached wrappers around discovery and the diff computation.

Streamlit reruns the whole script on every widget interaction, so anything that talks to two databases
has to be cached or the page re-queries on each click. A 5-minute TTL keeps it honest: a rebuilt step's
new metadata shows up shortly without anyone hunting for a refresh button.

Engine arguments are prefixed with `_` so Streamlit skips them when hashing (they aren't hashable, and
a session only ever has one pair). `cache_key` is the deliberate part of the key — pass something that
changes when the underlying data should be re-read.
"""

from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff import discovery
from apps.wizard.app_pages.metadata_diff.core import ViewDiff
from apps.wizard.app_pages.metadata_diff.usage import charts_using_indicators, mdims_using_indicators


@st.cache_data(ttl=300, show_spinner="Looking for metadata changes on this staging server…")
def summary(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> discovery.Summary:
    """Counts behind the section badges (and the same numbers owidbot reports)."""
    return discovery.summarize(_source_engine, _target_engine)


@st.cache_data(ttl=300, show_spinner="Checking which MDims changed…")
def mdim_changes(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> pd.DataFrame:
    """MDim list + change flags, indexed by catalogPath."""
    return discovery.mdim_changes_df(_source_engine, _target_engine)


@st.cache_data(ttl=300, show_spinner="Computing metadata diff for all views…")
def mdim_view_diffs(
    catalog_path: str,
    _source_engine: Engine,
    _target_engine: Engine,
    cache_key: str = "",
) -> tuple[list[dict[str, Any]], list[ViewDiff]]:
    """(dimensions, per-view diffs) for one MDim. `cache_key` should carry its config hashes."""
    config = discovery.load_mdim_config(_source_engine, catalog_path)
    assert config is not None, f"MDim {catalog_path} not found in staging."
    return config.get("dimensions") or [], discovery.mdim_text_changes(_source_engine, _target_engine, catalog_path)


@st.cache_data(ttl=300, show_spinner="Finding indicators whose text changed…")
def indicator_changes(
    _source_engine: Engine, _target_engine: Engine, cache_key: str = ""
) -> discovery.IndicatorChanges:
    """Indicators used by published charts whose user-visible text this branch changed."""
    return discovery.changed_indicators(_source_engine, _target_engine)


@st.cache_data(ttl=300, show_spinner="Checking which side changed each dataset…")
def indicator_attribution(
    _source_engine: Engine,
    _target_engine: Engine,
    catalog_paths: tuple[str, ...],
    cache_key: str = "",
) -> dict[str, str]:
    """Per changed indicator: is the difference this branch's, the baseline's, or both?"""
    return discovery.attribute_indicator_changes(_source_engine, _target_engine, list(catalog_paths))


@st.cache_data(ttl=300, show_spinner="Finding explorer views whose text changed…")
def explorer_changes(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> discovery.ExplorerChanges:
    """Published explorers whose view text changed, split into this branch's and baseline lag."""
    return discovery.changed_explorer_views(_source_engine, _target_engine)


@st.cache_data(ttl=300, show_spinner="Finding affected charts and MDims…")
def usage_for_indicators(
    indicator_ids: tuple[int, ...],
    catalog_path: str,
    _source_engine: Engine,
    cache_key: str = "",
) -> dict[int, dict[str, list[dict[str, Any]]]]:
    """For each changed indicator, the charts and *other* MDims on this server that use it."""
    ids = list(indicator_ids)
    if not ids:
        return {}
    charts = charts_using_indicators(_source_engine, ids)
    mdims = mdims_using_indicators(_source_engine, ids, exclude_catalog_path=catalog_path)
    return {i: {"charts": charts.get(i, []), "mdims": mdims.get(i, [])} for i in ids}
