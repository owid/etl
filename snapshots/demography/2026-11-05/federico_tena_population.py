"""Snapshot of the Federico–Tena World Population Historical Database at 1991 borders.

This is the November 2026 revision, which the authors sent us directly. It is not in the
public e-cienciaDatos repository yet — doi:10.21950/GW7SOZ still holds the January 2026
release (datafile 51077, tab-separated) that snapshot ``demography/2026-05-20`` ingests.
Re-point this snapshot at ``url_download`` once the revision is published there.

The repository also sits behind an Anubis JS proof-of-work bot wall, so even the published
files cannot be fetched unattended: an automated download stores the challenge HTML page
instead of the data. Either way, pass the local path:

    .venv/bin/etls demography/2026-11-05/federico_tena_population --path-to-file <path>/*.xlsx

Workbook layout (sheet "COUNTRIES BY CONTINENT"): row 0 continent labels, row 1 country names
at 1991 borders, row 2 the "(000's omitted)" unit marker, rows 3-141 one year each (1800-1938).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(path_to_file: str, upload: bool = True) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
