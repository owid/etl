"""Garden step for fossil fuel production, combining several sources by priority.

Coal, oil and gas production come from (in priority order):
- Energy Institute Statistical Review of World Energy (from 1965).
- Etemad & Luciani (1900-1979), for the historical fill before the Statistical Review.
- U.S. EIA International Energy (1949-2025), for country breadth and recent years not covered by the
  Statistical Review (this replaces the role the Shift Data Portal used to play, and fixes the 2016
  truncation for the many countries the Statistical Review does not report).
- Smil (2017), for World coal before 1900.
- NIC / Fouquet UK historical energy, for United Kingdom coal before 1900.

Etemad is prioritized over EIA for the pre-1965 fill so the historical series joins the Statistical
Review without the step that EIA's (slightly higher) values would introduce at the 1965 splice.

The dataset also includes the World reserves-to-production ratio for each fossil fuel.
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Reserves/production ratio conversion factors (from the "Approximate conversion factors" sheet).
BILLION_BARRELS_TO_TONNES = 1e9 * 0.1364
MILLION_TONNES_TO_TONNES = 1e6
TRILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e12
BILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e9

# Year from which the Statistical Review covers fossil fuel production.
STATISTICAL_REVIEW_FIRST_YEAR = 1965
# Year before which the historical (Smil / Fouquet) coal data is used.
HISTORICAL_LAST_YEAR = 1900

FUELS = ["coal", "oil", "gas"]


def prepare_statistical_review_data(tb_review: Table) -> Table:
    tb = tb_review.reset_index()[["country", "year", "coal_production_twh", "oil_production_twh", "gas_production_twh"]]
    return tb


def prepare_etemad_data(tb_etemad: Table) -> Table:
    tb = tb_etemad.reset_index()[["country", "year", "coal_production_twh", "oil_production_twh", "gas_production_twh"]]
    return tb


def prepare_eia_data(tb_eia: Table) -> Table:
    columns = {
        "country": "country",
        "year": "year",
        "energy_production_from_coal": "coal_production_twh",
        "energy_production_from_petroleum": "oil_production_twh",
        "energy_production_from_natural_gas": "gas_production_twh",
    }
    tb = tb_eia.reset_index()[list(columns)].rename(columns=columns, errors="raise")
    # Drop EIA's own regional aggregates (marked with an "(EIA)" suffix).
    tb = tb[~tb["country"].str.contains("(EIA)", regex=False)].reset_index(drop=True)
    return tb


def prepare_smil_data(tb_smil: Table) -> Table:
    # Smil only reports World coal (as direct energy, which equals production).
    tb = tb_smil.reset_index()[["country", "year", "coal__twh_direct_energy"]].rename(
        columns={"coal__twh_direct_energy": "coal_production_twh"}, errors="raise"
    )
    tb = tb.dropna(subset=["coal_production_twh"]).reset_index(drop=True)
    return tb


def prepare_uk_historical_data(tb_uk: Table) -> Table:
    tb = tb_uk.reset_index()[["country", "year", "coal_production_twh"]]
    return tb


def combine_production_data(tb_review: Table, tb_etemad: Table, tb_eia: Table, tb_historical: Table) -> Table:
    index_columns = ["country", "year"]
    for tb, name in [(tb_review, "Statistical Review"), (tb_etemad, "Etemad & Luciani"), (tb_eia, "EIA")]:
        assert not tb.duplicated(subset=index_columns).any(), f"Duplicated (country, year) rows in {name} data."

    # Combine by priority: Statistical Review > Etemad & Luciani > EIA.
    combined = combine_two_overlapping_dataframes(df1=tb_review, df2=tb_etemad, index_columns=index_columns)
    combined = combine_two_overlapping_dataframes(df1=combined, df2=tb_eia, index_columns=index_columns)

    # Extend coal production before 1900 with the historical data (Smil for the World, Fouquet for the UK),
    # giving it the lowest priority.
    combined = combine_two_overlapping_dataframes(df1=combined, df2=tb_historical, index_columns=index_columns)

    # Remove rows that only have nans.
    combined = combined.dropna(subset=[f"{fuel}_production_twh" for fuel in FUELS], how="all")
    combined = combined.sort_values(index_columns).reset_index(drop=True)
    return combined


def add_annual_change(tb: Table) -> Table:
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    # Only consider changes between consecutive years (the World coal series is decadal before 1900).
    is_consecutive = tb.groupby("country", observed=True)["year"].diff() == 1
    for fuel in FUELS:
        pct_change = tb.groupby("country", observed=True)[f"{fuel}_production_twh"].pct_change(fill_method=None) * 100
        abs_change = tb.groupby("country", observed=True)[f"{fuel}_production_twh"].diff()
        tb[f"{fuel}_production_annual_change_pct"] = pct_change.where(is_consecutive)
        tb[f"{fuel}_production_annual_change_twh"] = abs_change.where(is_consecutive)
    return tb


def add_per_capita(tb: Table) -> Table:
    expected_countries_without_population = [
        country for country in tb["country"].unique() if ("(EI)" in country) or ("(EIA)" in country)
    ]
    tb = paths.regions.add_population(
        tb=tb,
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=expected_countries_without_population,
    )
    for fuel in FUELS:
        tb[f"{fuel}_production_per_capita_kwh"] = tb[f"{fuel}_production_twh"] / tb["population"] * TWH_TO_KWH
    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def add_reserves_to_production_ratio(tb: Table, tb_review: Table) -> Table:
    """Add World reserves-to-production ratios (years of fossil fuels left), from the Statistical Review."""
    tb_review = tb_review.reset_index()
    columns = [
        "coal_reserves_mt",
        "coal_production_mt",
        "oil_reserves_bbl",
        "oil_production_mt",
        "gas_reserves_tcm",
        "gas_production_bcm",
    ]
    world = tb_review[tb_review["country"] == "World"][["country", "year"] + columns].copy()

    # Convert reserves and production to common units.
    world["coal_reserves"] = world["coal_reserves_mt"] * MILLION_TONNES_TO_TONNES
    world["coal_production"] = world["coal_production_mt"] * MILLION_TONNES_TO_TONNES
    world["oil_reserves"] = world["oil_reserves_bbl"] * BILLION_BARRELS_TO_TONNES
    world["oil_production"] = world["oil_production_mt"] * MILLION_TONNES_TO_TONNES
    world["gas_reserves"] = world["gas_reserves_tcm"] * TRILLION_CUBIC_METERS_TO_CUBIC_METERS
    world["gas_production"] = world["gas_production_bcm"] * BILLION_CUBIC_METERS_TO_CUBIC_METERS

    # Reserves-to-production ratio (years of fossil fuels left at current production).
    for fuel in FUELS:
        world[f"{fuel}_reserves_to_production_ratio"] = world[f"{fuel}_reserves"] / world[f"{fuel}_production"]

    world = world[["country", "year"] + [f"{fuel}_reserves_to_production_ratio" for fuel in FUELS]]

    # Merge the World ratio columns into the main table.
    tb = tb.merge(world, on=["country", "year"], how="left")
    return tb


def add_variable_metadata(tb: Table) -> Table:
    fuel_names = {"coal": "Coal", "oil": "Oil", "gas": "Gas"}
    for fuel, name in fuel_names.items():
        specs = {
            f"{fuel}_production_twh": (f"{name} production", "terawatt-hours", "TWh"),
            f"{fuel}_production_per_capita_kwh": (
                f"{name} production per capita",
                "kilowatt-hours per person",
                "kWh",
            ),
            f"{fuel}_production_annual_change_twh": (
                f"Annual change in {name.lower()} production",
                "terawatt-hours",
                "TWh",
            ),
            f"{fuel}_production_annual_change_pct": (f"Annual change in {name.lower()} production", "%", "%"),
            f"{fuel}_reserves_to_production_ratio": (
                f"{name} reserves-to-production ratio",
                "years",
                "years",
            ),
        }
        for column, (title, unit, short_unit) in specs.items():
            if column in tb.columns:
                tb[column].metadata.title = title
                tb[column].metadata.unit = unit
                tb[column].metadata.short_unit = short_unit
    return tb


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."
    for fuel in FUELS:
        assert (tb[f"{fuel}_production_twh"].dropna() >= 0).all(), f"Negative {fuel} production found."

    # Coal coverage after extending with historical data: World from 1800, United Kingdom from 1700.
    coal = tb.dropna(subset=["coal_production_twh"])
    assert coal[coal["country"] == "World"]["year"].min() == 1800, "World coal coverage changed."
    assert coal[coal["country"] == "United Kingdom"]["year"].min() == 1700, "UK coal coverage changed."

    # Continuity across the pre-1900 -> modern splice for the UK (which has annual coal data around 1900).
    uk_coal = coal[coal["country"] == "United Kingdom"].set_index("year")["coal_production_twh"]
    rel_diff = abs(uk_coal.loc[1900] - uk_coal.loc[1899]) / uk_coal.loc[1900]
    assert rel_diff < 0.10, f"Discontinuity in UK coal production at the 1899->1900 splice ({rel_diff:.0%})."


def run() -> None:
    #
    # Load data.
    #
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    ds_etemad = paths.load_dataset("etemad_luciani")
    tb_etemad = ds_etemad.read("etemad_luciani", reset_index=False)

    ds_eia = paths.load_dataset("international_energy")
    tb_eia = ds_eia.read("international_energy", reset_index=False)

    ds_smil = paths.load_dataset("smil_2017")
    tb_smil = ds_smil.read("smil_2017")

    ds_uk = paths.load_dataset("uk_historical_energy")
    tb_uk = ds_uk.read("uk_historical_energy")

    #
    # Process data.
    #
    tb_review_prod = prepare_statistical_review_data(tb_review=tb_review)
    tb_etemad = prepare_etemad_data(tb_etemad=tb_etemad)
    tb_eia = prepare_eia_data(tb_eia=tb_eia)

    # Historical coal (Smil for the World, Fouquet for the UK), restricted to before 1900.
    tb_smil = prepare_smil_data(tb_smil=tb_smil)
    tb_uk = prepare_uk_historical_data(tb_uk=tb_uk)
    tb_historical = pr.concat([tb_smil, tb_uk], ignore_index=True)
    tb_historical = tb_historical[tb_historical["year"] < HISTORICAL_LAST_YEAR].reset_index(drop=True)

    # Combine all production sources.
    tb = combine_production_data(
        tb_review=tb_review_prod, tb_etemad=tb_etemad, tb_eia=tb_eia, tb_historical=tb_historical
    )

    # Add annual change and per-capita variables.
    tb = add_annual_change(tb=tb)
    tb = add_per_capita(tb=tb)

    # Add World reserves-to-production ratios.
    tb = add_reserves_to_production_ratio(tb=tb, tb_review=tb_review)

    # Set variable metadata.
    tb = add_variable_metadata(tb=tb)

    # Sanity checks.
    sanity_check_outputs(tb=tb)

    # Format table conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
