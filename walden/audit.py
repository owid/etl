#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  audit.py
#  walden
#

from pathlib import Path

import click
import jsonschema
import requests

from owid.walden import catalog


@click.command()
def audit() -> None:
    "Audit files in the index against the schema."
    schema = catalog.load_schema()

    i = 0
    for filename, document in catalog.iter_docs():
        print(Path(filename).relative_to(catalog.INDEX_DIR))
        jsonschema.validate(document, schema)

        if "source_data_url" in document:
            check_url(document["source_data_url"])
        if "owid_data_url" in document:
            check_url(document["owid_data_url"])
        i += 1

    print(f"{i} catalog entries ok, all urls ok")


def check_url(url: str) -> None:
    "Make sure the URL is still valid."
    resp = requests.head(url)
    if resp.status_code not in (200, 301, 302):
        raise InvalidOrExpiredUrl(url)


class InvalidOrExpiredUrl(Exception):
    pass


if __name__ == "__main__":
    audit()
