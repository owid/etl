import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # retrieve snapshot
    snap: Snapshot = paths.load_dependency("unodc.xlsx")

    df = pd.read_excel(snap.path, skiprows=2)

    # clean and transform data
    df = clean_data(df)

    # reset index so the data can be saved in feather format
    df = df.reset_index(drop=True)

    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(
        ["country", "region", "subregion", "dimension", "category", "sex", "age", "year", "unit_of_measurement"],
        verify_integrity=True,
    )
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()

    log.info("unodc.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        (df["Dimension"].isin(["Total", "by mechanisms", "by relationship to perpetrator", "by situational context"]))
        & (
            df["Indicator"].isin(
                ["Victims of intentional homicide", "Victims of Intentional Homicide - Regional Estimate"]
            )
        )
    ]
    df = df.rename(
        columns={
            "Country": "country",
            "Year": "year",
        },
        errors="raise",
    ).drop(columns=["Iso3_code"])
    return df
