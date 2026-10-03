"""Script to create a snapshot of dataset.

The data file is extracted manually from the supplementary information of the paper
"Top wealth shares in the UK over more than a century" (Alvaredo, Atkinson and Morelli,
2018), available at https://doi.org/10.1016/j.jpubeco.2018.02.008.

Run with:
  etls economics/2026-07-28/wealth_uk --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
