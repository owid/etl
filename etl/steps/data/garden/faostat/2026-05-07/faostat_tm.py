"""FAOSTAT garden step for the Detailed Trade Matrix (faostat_tm)."""

import structlog
from owid.catalog import Table

from etl.helpers import PathFinder

log = structlog.get_logger()
paths = PathFinder(__file__)

# Map raw meadow column names → cleaner garden names.
COLUMN_RENAMES = {
    "reporter_countries": "reporter_country",
    "partner_countries": "partner_country",
}

# Final bilateral index for the garden table.
INDEX_COLUMNS = [
    "reporter_country",
    "partner_country",
    "item_code",
    "element_code",
    "year",
]

# Flag definitions.
FLAGS = {
    "A": "Official figure",
    "X": "Figure from external organization",
    "E": "Estimated value",
    "I": "Value imputed by a receiving agency",
}

# Earliest year expected in the FAOSTAT detailed trade matrix.
MIN_YEAR = 1986


def _count_self_trade(tb: Table) -> int:
    """Count rows where reporter and partner are the same country."""
    rep, par = tb["reporter_country"], tb["partner_country"]

    # Align categories so we can compare the integer codes directly. This
    # avoids materializing 50 M+ Python strings for the comparison.
    cats = sorted(set(rep.cat.categories) | set(par.cat.categories))
    rep_codes = rep.cat.set_categories(cats).cat.codes
    par_codes = par.cat.set_categories(cats).cat.codes
    return int((rep_codes == par_codes).sum())


def sanity_check_inputs(tb: Table) -> None:
    # All flag codes have a known definition.
    missing_flags = set(tb["flag"].cat.categories) - set(FLAGS)
    assert not missing_flags, f"Missing flag definitions: {sorted(missing_flags)}"

    # Values are non-null and non-negative. FAOSTAT trade flows should always
    # be ≥ 0; nulls would indicate a parsing problem upstream.
    assert tb["value"].notna().all(), "Found null values in 'value' column."
    assert (tb["value"] >= 0).all(), "Found negative values in 'value' column."

    # FAOSTAT TM contains a small but non-zero number of self-trade rows (where reporter == partner).
    assert _count_self_trade(tb) / len(tb) < 0.01, "Unexpectedly high percentage of self-trade rows."

    # Year range is sensible.
    min_year, max_year = int(tb["year"].min()), int(tb["year"].max())
    assert MIN_YEAR <= min_year <= max_year, f"Unexpected year range: {min_year}-{max_year}"

    error = "Unexpected units."
    assert set(tb["unit"]) == {"1000 An", "1000 USD", "An", "No", "t"}, error

    # For some reason, about 12% of rows report 0 tonnes of flow.
    error = "Unexpectedly high share of rows with zero value."
    assert 100 * len(tb[(tb["value"] == 0) & (tb["unit"] == "t")]) / len(tb[tb["unit"] == "t"]) < 13, error


def run() -> None:
    #
    # Load data.
    #
    # Use `safe_types=False` to save time and memory.
    ds_meadow = paths.load_dataset("faostat_tm")
    tb = ds_meadow.read("faostat_tm", safe_types=False)

    #
    # Process data.
    #
    # Rename columns conveniently.
    tb = tb.rename(columns=COLUMN_RENAMES, errors="raise")

    # Harmonize reporter and partner country names.
    tb = paths.regions.harmonize_names(tb=tb, country_col="reporter_country", warn_on_unused_countries=False)
    tb = paths.regions.harmonize_names(tb=tb, country_col="partner_country", warn_on_unused_countries=False)

    # Sanity check inputs.
    sanity_check_inputs(tb=tb)

    # Map flags.
    tb["flag"] = tb["flag"].cat.rename_categories(FLAGS)
    tb["flag"] = tb["flag"].copy_metadata(tb["value"])

    # Improve table format.
    tb = tb.format(keys=INDEX_COLUMNS)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
