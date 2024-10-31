from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # retrieve snapshot
    snap = paths.load_snapshot("unodc.xlsx")

    tb = snap.read(skiprows=2)

    # clean and transform data
    tb = clean_data(tb)

    # reset index so the data can be saved in feather format
    tb = tb.format(["country", "year", "indicator", "dimension", "category", "sex", "age", "unit_of_measurement"])

    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def clean_data(tb: Table) -> Table:
    tb = tb[
        (tb["Dimension"].isin(["Total", "by mechanisms", "by relationship to perpetrator", "by situational context"]))
        & (
            tb["Indicator"].isin(
                ["Victims of intentional homicide", "Victims of Intentional Homicide - Regional Estimate"]
            )
        )
    ]
    tb = tb.rename(
        columns={
            "Country": "country",
            "Year": "year",
        },
        errors="raise",
    )
    tb = tb.drop(columns=["Iso3_code", "Region", "Subregion"])
    return tb
