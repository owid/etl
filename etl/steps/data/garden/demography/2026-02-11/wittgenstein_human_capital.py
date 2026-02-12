"""Load a meadow dataset and create a garden dataset.

NOTES (January 2026):
- tb_proj: Contains data from 2020 onwards
- tb_hist: Contains historical data (starting 1950/1955), and often also data after 2020. We deal with this when we run .drop_duplicates() after concatenation.
"""

from typing import List

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

from shared import get_index_columns

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to add as aggregates
REGIONS = [
    "Africa",
    "North America",
    "South America",
    "Asia",
    "Europe",
    "Oceania",
]
# Not all columns are present in historical and projections datasets. This dictionary contains the expected differences.
TABLE_COLUMN_DIFFERENCES = {
    "by_edu": {
        "missing_in_hist": {"macb", "net"},
    },
    "by_sex_age": {
        "missing_in_proj": {"net"},
    },
    "main": {
        "missing_in_hist": {"emi", "imm"},
        "missing_in_proj": {"macb"},
    },
}
# Tables where we merge extra columns from historical data (columns that only exist in hist)
TABLES_MERGE_FROM_HIST = {
    "by_sex_age": ["net"],  # net only exists in historical
}
DTYPES = {
    "sex": "category",
    "age": "category",
    "education": "category",
    "country": "category",
    "year": "UInt16",
    "scenario": "UInt8",
}


def combine_tables_with_extra_hist_cols(
    tb_proj: Table, tb_hist: Table, extra_cols: List[str], index_cols: List[str]
) -> Table:
    """Combine tables, adding extra columns from historical via merge.

    This handles the case where historical data has columns that projections don't.
    We merge on index columns to add the historical-only columns.

    Args:
        tb_proj: Projection table (has priority for common columns)
        tb_hist: Historical table (source for extra columns)
        extra_cols: List of column names to add from historical
        index_cols: Index columns for merging

    Returns:
        Combined table with extra columns from historical
    """
    # Get common columns (excluding extra_cols that only exist in hist)
    columns_common = tb_proj.columns.intersection(tb_hist.columns)

    # Step 1: Concatenate common columns (proj first for priority)
    tb = pr.concat([tb_proj[columns_common], tb_hist[columns_common]], ignore_index=True)
    tb = tb.drop_duplicates(subset=index_cols, keep="first")

    # Step 2: Merge in extra columns from historical
    # Select only index + extra columns from historical, drop NAs in extra cols
    tb_hist_extra = tb_hist[index_cols + extra_cols].dropna(subset=extra_cols)

    # Merge
    tb = tb.merge(tb_hist_extra, on=index_cols, how="left")

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_proj = paths.load_dataset("wittgenstein_human_capital_proj")
    ds_hist = paths.load_dataset("wittgenstein_human_capital_historical")

    # Read table from meadow dataset.
    paths.log.info("reading tables...")
    tbs_proj = {t.m.short_name: t.reset_index() for t in ds_proj}
    tbs_hist = {t.m.short_name: t.reset_index() for t in ds_hist}

    #
    # Processing
    #
    assert tbs_proj.keys() == tbs_hist.keys(), "Mismatch in tables between historical and projection datasets"

    # Build all tables (without formatting, so we can do cross-table TFR computation)
    tables_raw = {}
    for key in tbs_proj.keys():
        paths.log.info(f"Building {key}")

        # Get tables
        tb_proj = tbs_proj[key]
        tb_hist = tbs_hist[key]

        # Dtypes
        tb_proj = tb_proj.astype({k: v for k, v in DTYPES.items() if k in tb_proj.columns})
        tb_hist = tb_hist.astype({k: v for k, v in DTYPES.items() if k in tb_hist.columns})

        # Check
        sanity_checks(tb_proj, tb_hist)

        # Get index columns
        index = get_index_columns(tb_proj)

        if key in TABLES_MERGE_FROM_HIST:
            # Special case: merge extra columns from historical
            extra_cols = TABLES_MERGE_FROM_HIST[key]
            tb = combine_tables_with_extra_hist_cols(tb_proj, tb_hist, extra_cols, index)
        else:
            # Standard case: intersection of columns
            columns_common = tb_proj.columns.intersection(tb_hist.columns)
            tb = pr.concat([tb_proj[columns_common], tb_hist[columns_common]], ignore_index=True)
            tb = tb.drop_duplicates(subset=index, keep="first")

        # Harmonize country names
        tb = paths.regions.harmonize_names(tb=tb)

        # Add region aggregates where applicable
        if key == "by_sex_age_edu":
            tb = paths.regions.add_aggregates(
                tb=tb,
                aggregations={"pop": "sum"},
                index_columns=["country", "year", "scenario", "sex", "age", "education"],
                regions=REGIONS,
                min_frac_countries_informed=0.7,
            )
        elif key == "by_sex_age":
            tb = paths.regions.add_aggregates(
                tb=tb,
                aggregations={"net": "sum"},
                index_columns=["country", "year", "scenario", "sex", "age"],
                regions=REGIONS,
                min_frac_countries_informed=0.7,
            )
            # World net migration is meaningless (closed system)
            tb.loc[tb["country"] == "World", "net"] = np.nan

        tables_raw[key] = tb

    # Compute regional TFR from ASFR + female pop (cross-table)
    paths.log.info("Computing regional TFR from ASFR + female pop")
    tables_raw = _compute_regional_tfr(tables_raw)

    # Format all tables
    tables = []
    for key, tb in tables_raw.items():
        index = get_index_columns(tb)
        tb = tb.format(index, short_name=key)

        # Reduce origins
        for col in tb.columns:
            tb[col].metadata.origins = [tb[col].metadata.origins[0]]

        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def _compute_regional_tfr(tables_raw: dict[str, Table]) -> dict:
    """Compute regional TFR from ASFR and female population data (cross-table).

    TFR is a rate and cannot be summed or averaged. We reconstruct it from:
    TFR_region = 5 × Σ_age( Σ_countries(ASFR × fpop) / Σ_countries(fpop) ) / 1000

    ASFR lives in by_age_edu, female pop in by_sex_age_edu. We merge, compute births, aggregate to regions, then derive TFR. add_aggregates handles filtering to member countries only, so source-level regions (World, UNSD) are automatically excluded.
    """
    # Get ASFR (only rows with data, i.e. reproductive ages)
    tb_asfr = (
        tables_raw["by_age_edu"]
        .loc[:, ["country", "year", "scenario", "age", "education", "asfr"]]
        .dropna(subset=["asfr"])
        .copy()
    )

    # Get female pop, filter to reproductive ages
    tb_fpop = tables_raw["by_sex_age_edu"]
    tb_fpop = tb_fpop.loc[
        tb_fpop["sex"] == "female", ["country", "year", "scenario", "age", "education", "pop"]
    ].rename(columns={"pop": "fpop"})

    # Merge and compute births (ASFR is per 1000 women)
    tb_temp = tb_asfr.merge(tb_fpop, on=["country", "year", "scenario", "age", "education"], how="inner")
    tb_temp["births"] = tb_temp["asfr"] * tb_temp["fpop"] / 1000
    tb_temp = tb_temp.loc[:, ["country", "year", "scenario", "age", "education", "births", "fpop"]]

    # Aggregate births and fpop to regions, keep only regional data
    tb_temp = paths.regions.add_aggregates(
        tb=tb_temp,
        aggregations={"births": "sum", "fpop": "sum"},
        index_columns=["country", "year", "scenario", "age", "education"],
        regions=REGIONS,
        min_frac_countries_informed=0.7,
    )
    tb_regional = tb_temp.loc[tb_temp["country"].isin(REGIONS)].copy()

    # Compute regional TFR: sum age-specific rates across age groups, multiply by 5
    tb_regional["rate"] = tb_regional["births"] / tb_regional["fpop"]
    tb_regional_tfr = (
        tb_regional.groupby(["country", "year", "scenario", "education"], observed=True)["rate"].sum().reset_index()
    )
    tb_regional_tfr["tfr"] = 5 * tb_regional_tfr["rate"]
    tb_regional_tfr = tb_regional_tfr.drop(columns=["rate"])
    tb_regional_tfr["tfr"] = tb_regional_tfr["tfr"].copy_metadata(tables_raw["by_edu"]["tfr"])

    # Add regional TFR to by_edu table
    tb_edu = tables_raw["by_edu"]
    assert not tb_edu["country"].isin(REGIONS).any(), "by_edu already contains region rows, but shouldn't"
    tables_raw["by_edu"] = pr.concat([tb_edu, tb_regional_tfr], ignore_index=True)

    return tables_raw


def sanity_checks(tb_proj, tb_hist):
    # Short name sanity check
    assert (
        tb_proj.m.short_name == tb_hist.m.short_name
    ), f"Mismatch in short_name of historical ({tb_hist.m.short_name}) and projection ({tb_proj.m.short_name})"
    key = tb_proj.m.short_name

    # Look for differences
    missing_in_hist = set(tb_proj.columns) - set(tb_hist.columns)
    missing_in_proj = set(tb_hist.columns) - set(tb_proj.columns)

    # Check with expected differences
    if key in TABLE_COLUMN_DIFFERENCES:
        missing_in_hist_expected = TABLE_COLUMN_DIFFERENCES[key].get("missing_in_hist", set())
        missing_in_proj_expected = TABLE_COLUMN_DIFFERENCES[key].get("missing_in_proj", set())
        assert missing_in_hist == missing_in_hist_expected, (
            f"Table {key}: Missing columns in historical dataset. "
            f"Expected: {missing_in_hist_expected}, Found: {missing_in_hist}"
        )
        assert missing_in_proj == missing_in_proj_expected, (
            f"Table {key}: Missing columns in projection dataset. "
            f"Expected: {missing_in_proj_expected}, Found: {missing_in_proj}"
        )
    else:
        assert set(tb_proj.columns) == set(tb_hist.columns), (
            f"Table {key}: Mismatch in columns between historical and projection. "
            f"Projection columns: {tb_proj.columns.tolist()}, Historical columns: {tb_hist.columns.tolist()}"
        )

    # Validate columns for TABLES_MERGE_FROM_HIST exist in historical
    if key in TABLES_MERGE_FROM_HIST:
        extra_cols = TABLES_MERGE_FROM_HIST[key]
        for col in extra_cols:
            assert (
                col in tb_hist.columns
            ), f"Table {key}: Expected column '{col}' in historical for merge, but not found"
