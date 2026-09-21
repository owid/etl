import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Conversion factor from million tonnes of oil equivalent to terawatt-hours.
MTOE_TO_TWH = 11.63

# Conversion factor from gigawatt-hours to terawatt-hours.
GWH_TO_TWH = 1e-3

# First year for which BEIS reports electricity supplied by all generators.
# Before this year, only major power producers are covered.
ALL_GENERATORS_FIRST_YEAR = 1951

# Generation series reported by BEIS on both scopes (major power producers and all generators),
# and the name of the combined output column built from them (all generators from 1951, major
# power producers before).
GENERATION_SERIES = {
    "total_generation": ("total_supplied_major_power_producers", "total_supplied_all_generators"),
    "conventional_thermal_generation": (
        "conventional_thermal_supplied_major_power_producers",
        "conventional_thermal_supplied_all_generators",
    ),
    "ccgt_generation": ("ccgt_supplied_major_power_producers", "ccgt_supplied_all_generators"),
    "nuclear_generation": ("nuclear_supplied_major_power_producers", "nuclear_supplied_all_generators"),
    "non_thermal_renewables_generation": (
        "hydro_natural_flow_supplied_major_power_producers",
        "non_thermal_renewables_supplied_all_generators",
    ),
}


def combine_tables(tb_fuel_input: Table, tb_supply: Table, tb_efficiency: Table) -> Table:
    """Combine tables (each one originally coming from a different sheet of the BEIS data file) and prepare output table
    with metadata.

    Parameters
    ----------
    tb_fuel_input : Table
        Data extracted from the "Fuel input" sheet.
    tb_supply : Table
        Data extracted from the "Supply, availability & consump" sheet.
    tb_efficiency : Table
        Data (on implied efficiency) extracted from the "Generated and supplied" sheet.

    Returns
    -------
    tb_combined : Table
        Combined and processed table with metadata and a verified index.

    """
    tb_fuel_input = tb_fuel_input.copy()
    tb_supply = tb_supply.copy()
    tb_efficiency = tb_efficiency.copy()

    # Remove rows with duplicated year.
    tb_fuel_input = tb_fuel_input.drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    tb_supply = tb_supply.drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    tb_efficiency = tb_efficiency.drop_duplicates(subset="year", keep="last").reset_index(drop=True)

    # Convert units of fuel input data.
    for column in tb_fuel_input.set_index("year").columns:
        tb_fuel_input[column] *= MTOE_TO_TWH

    # Convert units of the generation series (originally in GWh).
    for column in tb_efficiency.set_index("year").columns:
        if column != "implied_efficiency":
            tb_efficiency[column] *= GWH_TO_TWH

    # Combine dataframes.
    tb_combined = pr.merge(tb_fuel_input, tb_supply, how="outer", on="year", short_name=paths.short_name)
    tb_combined = pr.merge(tb_combined, tb_efficiency, how="outer", on="year")

    # Add a country column (even if there is only one country).
    tb_combined["country"] = "United Kingdom"

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_combined


def sanity_check_generation_series(tb: Table) -> None:
    """Check assumptions made when combining the two coverage scopes of BEIS' generation series."""
    tb = tb.reset_index()

    # Where both scopes are reported, nuclear should be identical (all UK nuclear stations are major power producers).
    both = tb.dropna(subset=["nuclear_supplied_major_power_producers", "nuclear_supplied_all_generators"])
    error = "Nuclear generation differs between major power producers and all generators."
    assert (both["nuclear_supplied_major_power_producers"] == both["nuclear_supplied_all_generators"]).all(), error

    # The all-generators series should be complete from 1951 onwards (nuclear is also reported before
    # then, as zero); this guarantees that filling their gaps with the major-power-producers series only
    # affects years before 1951.
    for column in [
        "total_supplied_all_generators",
        "conventional_thermal_supplied_all_generators",
        "nuclear_supplied_all_generators",
        "non_thermal_renewables_supplied_all_generators",
    ]:
        informed_years = set(tb.dropna(subset=[column])["year"])
        error = f"Expected '{column}' to be complete from {ALL_GENERATORS_FIRST_YEAR} onwards."
        assert set(range(ALL_GENERATORS_FIRST_YEAR, tb["year"].max() + 1)) <= informed_years, error

    # On each scope, the reported components should add up to the reported total (BEIS' own accounting),
    # on the years where each scope is used for the combined series (major power producers before 1951,
    # all generators from then on). Outside those years the major-power-producers columns miss some
    # components (hydro during 1962-1966, and wind and solar, reported in columns not loaded here).
    for scope, year_mask, total, components in [
        (
            "major power producers",
            tb["year"] < ALL_GENERATORS_FIRST_YEAR,
            "total_supplied_major_power_producers",
            [
                "conventional_thermal_supplied_major_power_producers",
                "ccgt_supplied_major_power_producers",
                "nuclear_supplied_major_power_producers",
                "hydro_natural_flow_supplied_major_power_producers",
                "pumped_storage_supplied_major_power_producers",
            ],
        ),
        (
            "all generators",
            tb["year"] >= ALL_GENERATORS_FIRST_YEAR,
            "total_supplied_all_generators",
            [
                "conventional_thermal_supplied_all_generators",
                "ccgt_supplied_all_generators",
                "nuclear_supplied_all_generators",
                "non_thermal_renewables_supplied_all_generators",
                "pumped_storage_supplied_all_generators",
            ],
        ),
    ]:
        informed = tb[year_mask].dropna(subset=[total])
        residual = (informed[components].sum(axis=1, min_count=1) - informed[total]).abs()
        error = f"Some years report a total but no components ({scope})."
        assert residual.notna().all(), error
        error = f"Generation components do not add up to the total ({scope})."
        assert (residual < 0.005).all(), error


def add_combined_generation_series(tb: Table) -> Table:
    """Build generation series covering the full period, using each year's widest reported scope.

    BEIS reports electricity supplied by all generators only from 1951; before that, only major power
    producers are covered. For each series, take the all-generators data where available and fall back
    to major power producers for the earlier years. All series switch scope on the same year, so within
    any given year they remain consistent with each other (and with their total).
    """
    tb = tb.copy()

    for combined_column, (mpp_column, all_generators_column) in GENERATION_SERIES.items():
        tb[combined_column] = tb[all_generators_column].fillna(tb[mpp_column])

    # Combine conventional thermal and CCGT into a single thermal (fossil-fired) generation series.
    # NOTE: CCGT is only reported (and only nonzero) from the early 1990s; "conventional thermal and
    # other" includes a small amount of thermally-generated renewables (e.g. waste and landfill gas).
    tb["thermal_generation"] = tb[["conventional_thermal_generation", "ccgt_generation"]].sum(axis=1, min_count=1)
    tb = tb.drop(columns=["conventional_thermal_generation", "ccgt_generation"], errors="raise")

    # Drop the scope-specific columns, which were only needed to build the combined series.
    scope_columns = sorted(set(column for pair in GENERATION_SERIES.values() for column in pair)) + [
        "pumped_storage_supplied_major_power_producers",
        "pumped_storage_supplied_all_generators",
    ]
    tb = tb.drop(columns=scope_columns, errors="raise")

    return tb


def run() -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("uk_historical_electricity")
    tb_fuel_input = ds_meadow.read("fuel_input")
    tb_supply = ds_meadow.read("supply")
    tb_efficiency = ds_meadow.read("efficiency")

    #
    # Process data.
    #
    # Clean and combine tables.
    tb = combine_tables(tb_fuel_input=tb_fuel_input, tb_supply=tb_supply, tb_efficiency=tb_efficiency)

    # Check assumptions made about the two coverage scopes of the generation series.
    sanity_check_generation_series(tb=tb)

    # Build generation series covering the full period, using each year's widest reported scope.
    tb = add_combined_generation_series(tb=tb)

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
