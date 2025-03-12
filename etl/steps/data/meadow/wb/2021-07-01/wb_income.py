from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # retrieve raw data
    snap = paths.load_snapshot("wb_income.xlsx")
    tb = snap.read()

    # Checks
    assert tb.Economy.value_counts().max() == 1

    # Drop columns and set Index
    tb.set_index(
        ["Economy"],
        inplace=True,
        verify_integrity=True,
    )

    tb = tb.rename(
        columns={
            "Other (EMU or HIPC)": "other_emu_or_hipc",
        }
    )

    tb.m.short_name = "wb_income_group"

    tb = tb.underscore()

    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()
