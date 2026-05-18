"""Shared HTTP session for outbound calls from internal ETL code.

Centralizes a recognizable ``User-Agent`` header so traffic from this repo
is distinguishable from generic ``python-requests`` / ``pandas`` clients in
our access logs.

Use this for calls that hit **OWID-owned infrastructure** (e.g. the data
catalog, grapher, files.ourworldindata.org, search.owid.io, Datasette,
admin API). Do not use it for calls to third-party hosts (GitHub, Notion,
Slack, source-data providers in ``snapshots/``, etc.) — those should keep
the default UA or use whatever the provider expects.

Usage
-----
    from etl.http import session as http_session

    resp = http_session.get(url, timeout=10)

For ``pandas`` HTTP reads, use :data:`STORAGE_OPTIONS`::

    import pandas as pd
    from etl.http import STORAGE_OPTIONS

    pd.read_feather(url, storage_options=STORAGE_OPTIONS)

For ``httpx`` clients, pass :data:`HEADERS` as the ``headers=`` kwarg::

    import httpx
    from etl.http import HEADERS

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        ...
"""

from __future__ import annotations

import platform

import requests

# The internal `etl` package version in pyproject.toml is a static placeholder
# (never bumped), so attaching it here would be misleading. If per-deploy
# attribution becomes useful, switch this to read a build/deploy env var.
USER_AGENT = f"owid-etl (python {platform.python_version()})"

#: Plain dict for ``requests.get(..., headers=HEADERS)`` and ``httpx`` clients.
HEADERS = {"User-Agent": USER_AGENT}

#: For ``pd.read_csv``/``read_feather``/``read_parquet`` over HTTP. pandas
#: forwards these as headers via fsspec's HTTP backend.
STORAGE_OPTIONS = {"User-Agent": USER_AGENT}


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


#: Module-level session — reuse for connection pooling.
session = _make_session()
