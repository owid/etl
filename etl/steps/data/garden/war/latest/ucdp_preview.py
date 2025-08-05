"""This step is almost identical to ucdp. However, it includes the latest data from CED, which is not yet available in the UCDP yearly release dataset.

It is good to keep these separate since CED data is still in preview, and might contain errors.

For more details on the processing pipeline, please refer to garden/war/2024-08-26/ucdp.
"""

import re
import sys
import types
from importlib import util
from pathlib import Path

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
LAST_YEAR = 2024
LAST_YEAR_PREVIEW = 2025

# Number of events with no location assigned (see function estimate_metrics_locations)
NUM_MISSING_LOCATIONS = 1230

# Catalog path of the main UCDP dataset. NOTE: Change this when there is a new UCDP stable (yearly) release.
VERSION_UCDP_STABLE = "2025-06-13"
CATALOG_PATH = f"garden/war/{VERSION_UCDP_STABLE}/ucdp"


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Load datasets
    ds_meadow = paths.load_dataset(
        short_name="ucdp", channel="meadow", namespace="war", version=VERSION_UCDP_STABLE
    )  # UCDP
    ds_gw = paths.load_dataset("gleditsch")  # Gleditsch
    ds_maps = paths.load_dataset("geoboundaries_cgaz")  # GeoBoundaries
    ds_population = paths.load_dataset("population")  # Population

    # Import UCDP module
    module_ucdp = import_ucdp_module(CATALOG_PATH)

    # Sanity checks (1)
    paths.log.info("sanity checks")
    module_ucdp._sanity_checks(ds_meadow)

    # Load tables
    tb_ged = ds_meadow.read("ucdp_ged")
    tb_conflict = ds_meadow.read("ucdp_battle_related_conflict")
    tb_dyadic = ds_meadow.read("ucdp_battle_related_dyadic")
    tb_prio = ds_meadow.read("ucdp_prio_armed_conflict")
    tb_regions = ds_gw.read("gleditsch_regions")
    tb_codes = ds_gw.read("gleditsch_countries")
    tb_maps = ds_maps.read("geoboundaries_cgaz")

    # Load candidate (preliminary) data
    ds_ced = paths.load_dataset("ucdp_ced")
    tb_ced = ds_ced.read("ucdp_ced")

    #
    # Adapt for with CED data
    #
    ## Extend codes to have data for latest years
    tb_codes = tb_codes.loc[tb_codes["year"] <= LAST_YEAR_PREVIEW].set_index(["id", "year"])
    ## Add CED data
    tb_ged = add_ced_data(tb_ged, tb_ced, LAST_YEAR, LAST_YEAR_PREVIEW)

    #
    # Run main code
    #
    tables = module_ucdp.run_pipeline(
        tb_ged=tb_ged,
        tb_conflict=tb_conflict,
        tb_dyadic=tb_dyadic,
        tb_prio=tb_prio,
        tb_regions=tb_regions,
        tb_codes=tb_codes,
        tb_maps=tb_maps,
        ds_population=ds_population,
        num_missing_location=NUM_MISSING_LOCATIONS,
        last_year=LAST_YEAR,
        last_year_preview=LAST_YEAR_PREVIEW,
        short_name=paths.short_name,
        tolerance_unk_ctype=0.01,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def import_ucdp_module(catalog_path: str = CATALOG_PATH):
    """To avoid re-implementing the same code as in ucdp.py, we import the module here.

    We need to do this unusual import because the module path contains numeric values.
    """
    step_uri = f"data://{catalog_path}"
    assert (
        step_uri in paths.dependencies
    ), f"ucdp_preview module relies on the code of step {step_uri}. The dag should list this step as a dependency!"

    submodule_path = Path(f"steps/data/{catalog_path}.py")
    submodule_dir = submodule_path.parent
    module_name = submodule_path.stem
    module_path = ETL_DIR / submodule_dir
    pkg_module_path = module_path / f"{module_name}.py"

    # Extract folder path and convert to a valid Python module name
    pkg_name = str(submodule_dir).replace("/", ".").replace("-", "_")
    pkg_name = re.sub(r"\.(\d)", r"._\1", pkg_name)
    pkg_name = f"etl.{pkg_name}"  # legal surrogate
    pkg_module_name = f"{pkg_name}.{module_name}"  # the actual file

    # ------------------------------------------------------------
    # 1.  Put the dated folder on sys.path   <<<<<<<<<<
    #     Now   `import shared`   will succeed if  shared.py
    #     lives right next to ucdp.py.
    # ------------------------------------------------------------
    # sys.path.insert(0, str(ver_dir))
    sys.path.append(str(module_path))

    # ------------------------------------------------------------
    # 2.  Ensure the normal parent packages exist
    # ------------------------------------------------------------
    def ensure_pkg(name, path):
        if name not in sys.modules:
            pkg = types.ModuleType(name)
            pkg.__path__ = [str(path)]
            sys.modules[name] = pkg

    parts = pkg_module_name.split(".")
    for i in range(len(parts) - 1):
        pkg_name = ".".join(parts[: i + 1])
        pkg_path = Path("/".join(parts[: i + 1]))
        ensure_pkg(pkg_name, pkg_path)

    # ------------------------------------------------------------
    # 3.  Create a virtual package that points at the dated folder
    # ------------------------------------------------------------
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(module_path)]
    sys.modules[pkg_name] = pkg

    # ------------------------------------------------------------
    # 4.  Load module
    # ------------------------------------------------------------
    spec = util.spec_from_file_location(pkg_module_name, pkg_module_path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {pkg_module_name}")

    module = util.module_from_spec(spec)
    sys.modules[pkg_module_name] = module
    spec.loader.exec_module(module)

    return module


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


def add_ced_data(tb_ged: Table, tb_ced: Table, last_year_ged: int, last_year_ced: int):
    # Merge CED into GED
    assert (tb_ced.columns == tb_ged.columns).all(), "Columns are not the same!"
    assert tb_ged["year"].max() == last_year_ged, "GED data is not up to date!"
    assert tb_ced["year"].max() == last_year_ced, "CED data is not up to date!"
    tb_ced = tb_ced[tb_ged.columns]
    tb_ged = pr.concat([tb_ged, tb_ced], ignore_index=True)

    return tb_ged
