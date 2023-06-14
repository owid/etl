#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  audit.py
#  walden
#

import concurrent.futures
import warnings
from pathlib import Path
from typing import Optional

import click
import jsonschema
import requests

from owid.walden import catalog


@click.command()
def audit() -> None:
    "Audit files in the index against the schema."
    schema = catalog.load_schema()

    # exclude backported datasets
    docs = [(f, doc) for f, doc in catalog.iter_docs() if "walden/index/backport" not in f]

    # speed it up with parallelization
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(
            lambda args: audit_doc(args[0], args[1], schema),
            docs,
        )

    print(f"{len(list(results))} catalog entries ok, all urls ok")


def audit_doc(filename: str, document: dict, schema: dict) -> None:
    print(Path(filename).relative_to(catalog.INDEX_DIR))
    jsonschema.validate(document, schema)

    if "owid_data_url" not in document:
        raise Exception(f"Missing 'owid_data_url' in {filename}")

    if "source_data_url" in document and document.get("is_public", True):
        check_url(document["owid_data_url"])
        check_url(document["source_data_url"], strict=False)


def check_url(url: str, strict: bool = True) -> None:
    "Make sure the URL is still valid."
    status_code: Optional[int]

    try:
        resp = requests.head(url)
        status_code = resp.status_code
    except requests.exceptions.SSLError:
        status_code = None
    except requests.exceptions.ConnectionError:
        status_code = None

    if status_code not in (200, 301, 302):
        if strict:
            raise InvalidOrExpiredUrl(url)
        else:
            warnings.warn(f"Invalid or expired URL: {url}")
            return


class InvalidOrExpiredUrl(Exception):
    pass


if __name__ == "__main__":
    audit()
