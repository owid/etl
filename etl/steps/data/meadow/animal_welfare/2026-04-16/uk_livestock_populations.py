"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("uk_livestock_populations.ods")
    tb_raw = snap.read_excel(sheet_name="Poultry", header=None)

    #
    # Process data.
    #
    # Row 2 contains years, row 5 contains "Hens and pullets laying eggs for eating".
    # The last column is "% change" and must be dropped.
    years = tb_raw.iloc[2, 1:].tolist()
    hens = tb_raw.iloc[5, 1:].tolist()

    df = pd.DataFrame({"year": years, "total_laying_hens": hens})

    # Drop the trailing "% change" column (non-integer year value).
    df = df[pd.to_numeric(df["year"], errors="coerce").notna()].copy()
    df["year"] = df["year"].astype(int)
    df["total_laying_hens"] = pd.to_numeric(df["total_laying_hens"], errors="coerce").round().astype("Int64")
    df["country"] = "United Kingdom"

    tb = pr.read_df(
        df=df[["country", "year", "total_laying_hens"]],
        metadata=snap.to_table_metadata(),
        origin=snap.metadata.origin,
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
