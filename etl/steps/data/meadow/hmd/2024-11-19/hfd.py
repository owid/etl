"""Load a snapshot and create a meadow dataset."""

import os
from pathlib import Path

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

indicators = {
    "cbr": {
        "name": "Crude birth rate",
    },
    "expos": "Female exposure to risk",
    "mi": "Conditional fertility rates",
}

# code year age
"""
x: code, year, age
o: code, year, ardy
$: code, cohort, x
£: code, cohort
dimensions_age = [

    adjtfrRR
    adjtfrRRbo

    x asfrRR
    x asfrRRbo
    x asfrTR
    x asfrTRbo
    x asfrVH
    x asfrVHbo
    o asfrVV
    o asfrVVbo

    x birthsRR
    x birthsRRbo
    x birthsTR
    x birthsTRbo
    x birthsVH
    x birthsVHbo
    o birthsVV
    o birthsVVbo

    cbrRR
    cbrRRbo

    x ccfrVH
    x ccfrVHbo

    $ cft

    x cpfrRR
    x cpfrRRbo
    o cpfrVV
    o cpfrVVbo

    x exposRR
    x exposRRpa
    x exposRRpac
    x exposTR
    x exposVH
    o exposVV

    mabRR
    mabRRbo
    mabVH
    mabVHbo

    x mi
    x mic

    patfr
    patfrc

    $ pft
    $ pftc

    pmab
    pmabc

    £ pprVHbo

    sdmabRR
    sdmabRRbo
    sdmabVH
    sdmabVHbo

    tfrRR
    tfrRRbo
    tfrVH
    tfrVHbo

    totbirthsRR
    totbirthsRRbo
]
"""
# code year ardy
dimensions_ardy = [
    "asfrVV",
    "asfrVVbo",
    "birthsVV",
    "birthsVVbo",
]
dimensions_custom = {
    "asfrRR":

    "exposRRpa": ["code", "year", "age"],
    "exposRRpac": ["code", "year", "age"],
    "mi": ["code", "year", "age"],
    "mic": ["code", "year", "age"],
    "pftc": ["code", "year", "x"],
    "sdmabVH": ["code", "cohort"],
    "sdmabVHbo": ["code", "cohort"],
    "cft": ["code", "cohort"],
    "pft": ["code", "year", "x"],
    "cpfrRRbo": ["code", "year", "age"],
    "cpfrVV": ["code", "year", "ardy"],
    "cpfrVVbo": ["code", "year", "ardy"],
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hfd.zip")

    # Load data from snapshot.
    tbs = []
    with snap.extract_to_tempdir() as tmp_dir:
        p = Path(tmp_dir)
        files = p.glob("Files/zip_w/*.txt")
        for f in files:
            # Read the content of the text file
            tb_ = pr.read_csv(
                f,
                delim_whitespace=True,
                skiprows=2,
            )
            tb_.m.short_name = f.stem
            # tb_ = tb_.format(["code", "year"])
            tbs.append(tb_)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
