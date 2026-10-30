"""Load the transcribed Volk & Atkinson (2013) mortality tables and create a meadow dataset.

The rates stay as the paper prints them - ranges, open-ended values and the one rate measured at a
different age. Turning them into numbers is garden's job, because the rule for doing so (midpoint of
a range, lower bound of an open-ended value) is a choice rather than a cleaning step.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Low-cardinality string columns, stored as categoricals to keep the feather small and fast to read.
CATEGORICAL_COLUMNS = ["imr", "cmr", "source"]


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("volk_atkinson_2013.csv")

    # keep_default_na=False is load-bearing: "n/a" is one of pandas' default NA strings, and reading
    # it as missing would erase the difference between a rate the paper prints as "n/a" and a cell it
    # leaves blank. That difference is what identifies China, 1700-1800, whose single printed value
    # sits in the child mortality column and whose infant mortality cell is empty.
    tb = snap.read_csv(dtype=str, keep_default_na=False)

    #
    # Process data.
    #
    tb["table"] = tb["table"].astype(int)
    for column in CATEGORICAL_COLUMNS:
        tb[column] = tb[column].astype("category")

    _sanity_check_transcription(tb)

    tb = tb.format(["table", "time", "culture"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def _sanity_check_transcription(tb) -> None:
    """Check the transcription still has the shape the paper prints.

    Row counts rather than values, because the values are checked against the PDF outside the
    pipeline. What this guards is a truncated or double-appended snapshot, which would otherwise
    change every average downstream without any step failing.
    """
    counts = tb.groupby("table", observed=True).size().to_dict()
    assert counts == {1: 22, 2: 46}, f"Expected 22 Table 1 rows and 46 Table 2 rows, got {counts}"

    # Each table prints its own N and Mean row, and Table 2 a final modern-comparison row.
    for table in (1, 2):
        cultures = set(tb.loc[tb["table"] == table, "culture"])
        assert {"N", "Mean"} <= cultures, f"Table {table} is missing its N or Mean row"
    assert (tb["time"] == "Modern").sum() == 1, "Expected exactly one modern-comparison row"

    # The published counts of societies carrying a child mortality rate, which the chart depends on.
    counts = tb[tb["culture"] == "N"]
    published_n = dict(zip(counts["table"], counts["cmr"].astype(str).astype(int)))
    assert published_n == {1: 17, 2: 24}, f"Published N for the CMR column changed: {published_n}"

    assert not tb.duplicated(["table", "time", "culture"]).any(), "Duplicate (table, time, culture)"
