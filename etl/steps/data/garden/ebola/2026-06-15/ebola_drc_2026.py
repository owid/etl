"""Garden step for the 2026 Bundibugyo Ebola outbreak (DRC + Uganda).

Produces two tables:
  - ``ebola_drc_2026``: national (DRC) time series, country-harmonized.
  - ``ebola_drc_2026_by_health_zone``: per-health-zone time series (subnational entities,
    not country-harmonized — the zone names are kept verbatim as grapher entities).
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

paths = PathFinder(__file__)
log = get_logger()

# Metrics expected from the snapshot (see snapshot script FILES mapping).
EXPECTED_METRICS = {
    "cumulative_confirmed_cases",
    "cumulative_confirmed_deaths",
    "cumulative_suspected_cases",
    "cumulative_suspected_deaths",
    "new_confirmed_cases",
    "new_suspected_cases",
    "new_suspected_deaths",
}


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["level"].unique()) <= {"national", "health_zone"}, "Unexpected level in source data."
    assert set(tb["metric"].unique()) <= EXPECTED_METRICS, "Unexpected metric in source data."
    assert not tb.duplicated(subset=["level", "metric", "location", "date"]).any(), (
        "Duplicate (level, metric, location, date) rows."
    )
    # National rows must all be DRC; anything else means the source changed its national label.
    nat_locations = set(tb.loc[tb["level"] == "national", "location"].unique())
    assert nat_locations <= {"DRC"}, f"Unexpected national location label(s): {nat_locations}"


def sanity_check_outputs(tb_nat: Table, tb_hz: Table) -> None:
    # No negative cases/deaths anywhere.
    for tb, name in [(tb_nat, "national"), (tb_hz, "health_zone")]:
        value_cols = [c for c in tb.columns if c not in ["country", "date"]]
        assert (tb[value_cols].min(numeric_only=True) >= 0).all(), f"Negative case/death count in {name} table."

    # National confirmed cases must be present and non-decreasing (cumulative).
    assert "cumulative_confirmed_cases" in tb_nat.columns, "National table is missing cumulative confirmed cases."
    nat = tb_nat.sort_values("date")
    cc = nat["cumulative_confirmed_cases"].dropna()
    if not cc.is_monotonic_increasing:
        # Soft signal: the source occasionally revises figures downward.
        log.warning("National cumulative confirmed cases are not monotonically increasing — possible source revision.")

    # Confirmed deaths should never exceed confirmed cases at the national level.
    overlap = nat.dropna(subset=["cumulative_confirmed_cases", "cumulative_confirmed_deaths"])
    assert (overlap["cumulative_confirmed_deaths"] <= overlap["cumulative_confirmed_cases"]).all(), (
        "National confirmed deaths exceed confirmed cases."
    )


def _pivot(tb: Table, short_name: str) -> Table:
    """Pivot long [location, date, metric, value] into wide [country, date, <metrics...>]."""
    tb = tb.pivot(index=["location", "date"], columns="metric", values="value", join_column_levels_with="_").rename(
        columns={"location": "country"}
    )
    # Drop columns that are entirely empty (e.g. confirmed deaths by zone are all "ND" early on).
    all_nan = [c for c in tb.columns if c not in ["country", "date"] and tb[c].isna().all()]
    if all_nan:
        log.info(f"Dropping fully-empty columns from {short_name}: {all_nan}")
        tb = tb.drop(columns=all_nan)
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("ebola_drc_2026")
    tb = ds_meadow["ebola_drc_2026"].reset_index()

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Parse the raw source strings: "ND"/"NA" -> NaN; dates -> datetime.
    tb["value"] = pr.to_numeric(tb["value"], errors="coerce")
    tb["date"] = pr.to_datetime(tb["date"], format="%Y-%m-%d")

    tb_nat = _pivot(tb[tb["level"] == "national"].copy(), "ebola_drc_2026")
    tb_hz = _pivot(tb[tb["level"] == "health_zone"].copy(), "ebola_drc_2026_by_health_zone")

    # Harmonize only the national table ("DRC" -> "Democratic Republic of Congo"). Health-zone
    # names are subnational and kept verbatim as grapher entities.
    tb_nat = paths.regions.harmonize_names(tb_nat, country_col="country", countries_file=paths.country_mapping_path)

    sanity_check_outputs(tb_nat, tb_hz)

    tb_nat = tb_nat.format(["country", "date"], short_name="ebola_drc_2026")
    tb_hz = tb_hz.format(["country", "date"], short_name="ebola_drc_2026_by_health_zone")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_nat, tb_hz], default_metadata=ds_meadow.metadata)
    ds_garden.save()
