"""FAOSTAT garden step for the Detailed Trade Matrix (faostat_tm)."""

import pandas as pd
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
    """Count rows where reporter and partner are the same country, robust to
    whether the columns are still categoricals (with potentially different
    category sets after harmonization) or have been decategorized to plain
    string columns by `harmonize_names`."""
    rep, par = tb["reporter_country"], tb["partner_country"]
    if isinstance(rep.dtype, pd.CategoricalDtype) and isinstance(par.dtype, pd.CategoricalDtype):
        # Align categories so we can compare the integer codes directly. This
        # avoids materializing 50 M+ Python strings for the comparison.
        cats = sorted(set(rep.cat.categories) | set(par.cat.categories))
        rep_codes = rep.cat.set_categories(cats).cat.codes
        par_codes = par.cat.set_categories(cats).cat.codes
        return int((rep_codes == par_codes).sum())
    return int((rep == par).sum())


def sanity_check_inputs(tb: Table) -> None:
    """Run cheap, deterministic checks on the harmonized but otherwise
    unprocessed input table. Anything that requires a self-join (e.g.
    quantifying reporting asymmetry between exporter and importer) is left
    to the analysis notebook in ai/faostat_tm/, since the answer depends on
    the year and the threshold is a judgement call."""
    # All flag codes have a known definition.
    missing_flags = set(tb["flag"].cat.categories) - set(FLAGS)
    assert not missing_flags, f"Missing flag definitions: {sorted(missing_flags)}"

    # Values are non-null and non-negative. FAOSTAT trade flows should always
    # be ≥ 0; nulls would indicate a parsing problem upstream.
    assert tb["value"].notna().all(), "Found null values in 'value' column."
    assert (tb["value"] >= 0).all(), "Found negative values in 'value' column."

    # FAOSTAT TM contains a small but non-zero number of self-trade rows (reporter == partner).
    n_self_trade = _count_self_trade(tb)
    self_trade_share = n_self_trade / len(tb) if len(tb) > 0 else 0
    log.info("faostat_tm.self_trade_rows", count=n_self_trade, share=f"{self_trade_share:.2%}")
    assert self_trade_share < 0.01, f"Self-trade rows are {self_trade_share:.2%} of the table."

    # Year range is sensible.
    min_year, max_year = int(tb["year"].min()), int(tb["year"].max())
    assert MIN_YEAR <= min_year <= max_year, f"Unexpected year range: {min_year}-{max_year}"


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
    # paths.regions.harmonizer(tb=tb, country_col="partner_country", institution="FAO")
    tb = paths.regions.harmonize_names(tb=tb, country_col="reporter_country", warn_on_unused_countries=False)
    tb = paths.regions.harmonize_names(tb=tb, country_col="partner_country", warn_on_unused_countries=False)

    # Sanity check inputs.
    # Diagnostic plots (year coverage, reporting coverage, quantity-agreement
    # bands) used to live here as commented-out dev aids; they have moved to
    # the analysis notebook at docs/analyses/food_trade/food_trade.ipynb.
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
