"""Snapshot of the 2026 Bundibugyo Ebola outbreak (DRC + Uganda) — core epidemiological indicators.

Source: INRB-UMIE/Ebola_DRC_2026 (Kraemer Lab, University of Oxford), which transcribes the
DRC Institut National de Santé Publique (INSP) SitRep MVE series PDFs into tidy per-metric CSVs
under ``build/long/``. Each file is headerless ``[location, date, value]`` where ``location`` is
either ``DRC`` (national files) or a health-zone name, ``date`` is ISO-8601, and ``value`` may carry
the source sentinel ``ND`` ("no data") which we keep verbatim — parsing happens in garden.

We download only the core case/death layer and stack the per-metric files into one long-format CSV
with explicit ``level`` and ``metric`` columns. The repository's mobility / conflict / demographic
layers (ACLED, IOM, Flowminder, WorldPop, …) are deliberately excluded: several carry restrictive
licenses and none are needed for a cases/deaths tracker.

GitHub is a third-party host, so this uses plain ``requests`` (default User-Agent), not the OWID session.
"""

from io import StringIO

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.helpers import PathFinder

paths = PathFinder(__file__)

BASE_URL = "https://raw.githubusercontent.com/INRB-UMIE/Ebola_DRC_2026/main/build/long/"

# Upstream file stem -> (level, metric). Every file is headerless [location, date, value].
FILES = {
    # National level (location == "DRC")
    "insp_sitrep__national_cumulative_confirmed_cases": ("national", "cumulative_confirmed_cases"),
    "insp_sitrep__national_cumulative_confirmed_deaths": ("national", "cumulative_confirmed_deaths"),
    "insp_sitrep__national_cumulative_suspected_cases": ("national", "cumulative_suspected_cases"),
    "insp_sitrep__national_cumulative_suspected_deaths": ("national", "cumulative_suspected_deaths"),
    # Health-zone level (location == health zone name)
    "insp_sitrep__cumulative_confirmed_cases": ("health_zone", "cumulative_confirmed_cases"),
    "insp_sitrep__cumulative_confirmed_deaths": ("health_zone", "cumulative_confirmed_deaths"),
    "insp_sitrep__cumulative_suspected_cases": ("health_zone", "cumulative_suspected_cases"),
    "insp_sitrep__cumulative_suspected_deaths": ("health_zone", "cumulative_suspected_deaths"),
    "insp_sitrep__new_confirmed_cases": ("health_zone", "new_confirmed_cases"),
    "insp_sitrep__new_suspected_cases": ("health_zone", "new_suspected_cases"),
    "insp_sitrep__new_suspected_deaths": ("health_zone", "new_suspected_deaths"),
}


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    snap = paths.init_snapshot()

    frames = []
    for stem, (level, metric) in FILES.items():
        resp = requests.get(f"{BASE_URL}{stem}.csv", timeout=60)
        resp.raise_for_status()
        # Keep values as strings so source sentinels ("ND"/"NA") survive into garden untouched.
        df = pd.read_csv(StringIO(resp.text), header=None, names=["location", "date", "value"], dtype=str)
        df["level"] = level
        df["metric"] = metric
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)[["level", "metric", "location", "date", "value"]]

    if combined.empty:
        raise ValueError("No data downloaded from the upstream Ebola repository.")

    df_to_file(combined, file_path=snap.path)  # ty: ignore[invalid-argument-type]

    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
