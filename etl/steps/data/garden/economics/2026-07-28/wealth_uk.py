"""Load a meadow dataset and create a garden dataset.

The source's supplementary file omits the top 0.1% and top 0.5-0.1% wealth shares for
four years (2002, 2005, 2006 and 2012). In those rows the remaining sub-bracket values
(top 10-5%, top 5-1%, top 1-0.5%) were written shifted one column to the left, so they
land in the wrong columns when the file is read. We detect those rows and shift the
values back into their correct columns; the two genuinely missing shares stay empty.
"""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Cumulative top-share columns, ordered from broadest to narrowest group.
CUMULATIVE = ["share_top_10", "share_top_5", "share_top_1", "share_top_0p5", "share_top_0p1"]
# Intermediate brackets and the two cumulative shares whose difference should reproduce them.
BRACKETS = {
    "share_top_10_5": ("share_top_10", "share_top_5"),
    "share_top_5_1": ("share_top_5", "share_top_1"),
    "share_top_1_0p5": ("share_top_1", "share_top_0p5"),
    "share_top_0p5_0p1": ("share_top_0p5", "share_top_0p1"),
}
SHARE_COLUMNS = ["share_bottom_90"] + CUMULATIVE + list(BRACKETS)
# Columns that are always populated in the raw file, even in the misaligned rows (the misalignment
# only ever drops the top 0.1% and top 0.5-0.1% shares and shifts the intermediate brackets).
CORE_ALWAYS_PRESENT = ["share_bottom_90", "share_top_10", "share_top_5", "share_top_1", "share_top_0p5"]


def fix_misaligned_rows(tb: Table) -> Table:
    """Shift the misread sub-bracket values back into their correct columns.

    In the affected rows `share_top_1_0p5` is empty and the three sub-bracket values were
    read into `share_top_0p1`, `share_top_10_5` and `share_top_5_1`. We move them back and
    leave `share_top_0p1` / `share_top_0p5_0p1` empty (they are genuinely absent in the source).
    """
    mask = tb["share_top_1_0p5"].isna() & tb["share_top_10_5"].notna()
    if mask.any():
        # Snapshot the shifted values before overwriting so the reassignment order can't corrupt them.
        v_top_10_5 = tb.loc[mask, "share_top_0p1"].to_numpy()
        v_top_5_1 = tb.loc[mask, "share_top_10_5"].to_numpy()
        v_top_1_0p5 = tb.loc[mask, "share_top_5_1"].to_numpy()
        tb.loc[mask, "share_top_10_5"] = v_top_10_5
        tb.loc[mask, "share_top_5_1"] = v_top_5_1
        tb.loc[mask, "share_top_1_0p5"] = v_top_1_0p5
        tb.loc[mask, "share_top_0p1"] = float("nan")
    return tb


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["country"].unique()) == {"United Kingdom"}, "Expected United Kingdom as the only entity."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["year"].between(1895, 2013).all(), "Year outside the expected 1895-2013 range."
    # The bottom 90% and the wider cumulative top shares are never missing in the source.
    assert tb[CORE_ALWAYS_PRESENT].notna().all().all(), "Unexpected missing values in a cumulative/bottom share."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    # All shares are percentages of total wealth.
    for col in SHARE_COLUMNS:
        vals = tb[col].dropna()
        assert vals.between(0, 100).all(), f"{col} has values outside 0-100%."
    # Bottom 90% and top 10% partition the population, so they must sum to 100%.
    assert (tb["share_bottom_90"] + tb["share_top_10"] - 100).abs().max() < 0.05, "Bottom 90% + top 10% != 100%."
    # Cumulative top shares must be non-increasing as the group narrows.
    for broader, narrower in zip(CUMULATIVE, CUMULATIVE[1:]):
        pair = tb[[broader, narrower]].dropna()
        assert (pair[broader] >= pair[narrower] - 0.05).all(), f"{broader} < {narrower} for some year."
    # Each intermediate bracket must equal the difference of the two cumulative shares around it.
    for bracket, (broader, narrower) in BRACKETS.items():
        check = tb[[bracket, broader, narrower]].dropna()
        residual = (check[bracket] - (check[broader] - check[narrower])).abs()
        assert residual.max() < 0.2, (
            f"{bracket} does not reconcile with {broader} - {narrower} (max {residual.max():.2f})."
        )


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("wealth_uk")
    tb = ds_meadow["wealth_uk"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)
    tb = fix_misaligned_rows(tb)

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
