#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  fetch.py
#  walden
#

from os import path

import click

from owid.walden import Catalog, ui


@click.command()
def fetch():
    """
    Fetch the full dataset file by file. Previously downloaded files are considered
    cached and are not re-downloaded.
    """
    for dataset in Catalog():
        if path.exists(dataset.local_path):
            ui.log("CACHED", dataset.local_path)

        else:
            dataset.ensure_downloaded()


if __name__ == "__main__":
    fetch()
