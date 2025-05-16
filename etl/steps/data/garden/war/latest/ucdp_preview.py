"""Data from UCDP.


Notes:
    - Conflict types for state-based violence is sourced from UCDP/PRIO dataset. non-state and one-sided violence is sourced from GED dataset.
    - There can be some mismatches with latest official reported data (UCDP's live dashboard). This is because UCDP uses latest data for their dashboard, which might not be available yet as bulk download.
    - Regions:
        - Uses `region` column for both GED and UCDP/PRIO datasets.
        - Incompatibilities in Oceania are encoded in "Asia". We therefore have changed the region name to "Asia and Oceania".
        - GED: Dataset uses names (not codes!)
            - You can learn more about the countries included in each region from section "Appendix 5 Main sources consulted during the 2022 update" in page 40,
            document: https://ucdp.uu.se/downloads/ged/ged231.pdf.
                - Note that countries from Oceania are included in Asia!
        - UCDP/PRIO: Dataset uses codes (note we changed "Asia" -> "Asia and Oceania")
            1 = Europe (GWNo: 200-399)
            2 = Middle East (GWNo: 630-699)
            3 = Asia (GWNo: 700-999)  [renamed to 'Asia and Oceania']
            4 = Africa (GWNo: 400-626)
            5 = Americas (GWNo: 2-199)
"""

import importlib.util
import sys

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder
from etl.paths import ETL_DIR

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping for Geo-referenced database
TYPE_OF_VIOLENCE_MAPPING = {
    2: "non-state conflict",
    3: "one-sided violence",
}
# Mapping for armed conflicts dataset (inc PRIO/UCDP)
UNKNOWN_TYPE_ID = 99
UNKNOWN_TYPE_NAME = "state-based (unknown)"
TYPE_OF_CONFLICT_MAPPING = {
    1: "extrasystemic",
    2: "interstate",
    3: "intrastate (non-internationalized)",
    4: "intrastate (internationalized)",
    UNKNOWN_TYPE_ID: UNKNOWN_TYPE_NAME,
}
# Regions mapping (for PRIO/UCDP dataset)
REGIONS_MAPPING = {
    1: "Europe",
    2: "Middle East",
    3: "Asia and Oceania",
    4: "Africa",
    5: "Americas",
}
REGIONS_EXPECTED = set(REGIONS_MAPPING.values())
# Last year of data
LAST_YEAR = 2023
LAST_YEAR_PREVIEW = 2024

# Number of events with no location assigned (see function estimate_metrics_locations)
NUM_MISSING_LOCATIONS = 2316


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Load datasets
    ds_meadow = paths.load_dataset(short_name="ucdp", channel="meadow", namespace="war", version="2024-08-26")  # UCDP
    ds_gw = paths.load_dataset("gleditsch")  # Gleditsch
    ds_maps = paths.load_dataset("nat_earth_110")  # Nat Earth
    ds_population = paths.load_dataset("population")  # Population

    # Import UCDP module
    ucdp_module = import_ucdp_module()

    # Sanity checks (1)
    paths.log.info("sanity checks")
    ucdp_module._sanity_checks(ds_meadow)

    # Load tables
    tb_ged = ds_meadow.read("ucdp_ged")
    tb_conflict = ds_meadow.read("ucdp_battle_related_conflict")
    tb_prio = ds_meadow.read("ucdp_prio_armed_conflict")
    tb_regions = ds_gw.read("gleditsch_regions")
    tb_codes = ds_gw["gleditsch_countries"]
    tb_maps = ds_maps.read("nat_earth_110")

    #
    # Adapt for with CED data
    #
    ## Extend codes to have data for latest years
    tb_codes = extend_latest_years(tb_codes, LAST_YEAR, LAST_YEAR_PREVIEW)
    ## Add CED data
    tb_ged = add_ced_data(tb_ged, LAST_YEAR, LAST_YEAR_PREVIEW)

    #
    # Run main code
    #
    ucdp_module.run_pipeline(
        tb_ged=tb_ged,
        tb_conflict=tb_conflict,
        tb_prio=tb_prio,
        tb_regions=tb_regions,
        tb_codes=tb_codes,
        tb_maps=tb_maps,
        ds_population=ds_population,
        default_metadata=ds_meadow.metadata,
        num_missing_location=NUM_MISSING_LOCATIONS,
        last_year=LAST_YEAR,
        last_year_preview=LAST_YEAR_PREVIEW,
    )


def import_ucdp_module():
    """To avoid re-implementing the same code as in ucdp.py, we import the module here.

    We need to do this unusual import because the module path contains numeric values.
    """
    catalog_path = "garden/war/2024-08-26/ucdp"
    step_uri = f"data://{catalog_path}"
    assert (
        step_uri in paths.dependencies
    ), f"ucdp_preview module relies on the code of step {step_uri}. The dag should list this step as a dependency!"

    # Import UCDP (latest) module
    module_path = ETL_DIR / f"steps/data/{catalog_path}.py"
    spec = importlib.util.spec_from_file_location("ucdp_module", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {module_path}")
    ucdp_module = importlib.util.module_from_spec(spec)
    sys.modules["ucdp_module"] = ucdp_module
    spec.loader.exec_module(ucdp_module)

    return ucdp_module


# CED-specific
def extend_latest_years(tb: Table, since_year, to_year) -> Table:
    """Create table with each country present in a year."""

    index = list(tb.index.names)
    tb = tb.reset_index()

    # define mask for last year
    mask = tb["year"] == since_year

    # Get year to extend to
    current_year = to_year

    tb_all_years = Table(pd.RangeIndex(since_year + 1, current_year + 1), columns=["year"])
    tb_last = tb[mask].drop(columns="year").merge(tb_all_years, how="cross")

    tb = pr.concat([tb, tb_last], ignore_index=True, short_name="gleditsch_countries")

    tb = tb.set_index(index)
    return tb


def add_ced_data(tb_ged: Table, last_year_ged: int, last_year_ced: int):
    # Read CED table
    ds_ced = paths.load_dataset("ucdp_ced")
    tb_ced = ds_ced.read("ucdp_ced")

    # Merge CED into GED
    assert (tb_ced.columns == tb_ged.columns).all(), "Columns are not the same!"
    assert tb_ged["year"].max() == last_year_ged, "GED data is not up to date!"
    assert tb_ced["year"].max() == last_year_ced, "CED data is not up to date!"
    tb_ced = tb_ced[tb_ged.columns]
    tb_ged = pr.concat([tb_ged, tb_ced], ignore_index=True)

    return tb_ged
