"""Garden step for Ember's Monthly Electricity Data.

The monthly file is Ember-only (no combining with other sources). Here we harmonize country names and
pivot the long-by-source data into the same wide, per-source column schema that the yearly Ember garden
produces (e.g. ``generation__coal__twh``, ``demand__total_demand__twh``), so the electricity mix garden
step can process the monthly data with the exact same functions it uses for the yearly data. The table
is indexed by date (monthly), not year.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Ember "Electricity source" -> wide generation column name (matching the yearly Ember garden schema,
# which the electricity mix garden's process_ember_data consumes).
GENERATION_SOURCE_TO_COLUMN = {
    "Bioenergy": "generation__bioenergy__twh",
    "Coal": "generation__coal__twh",
    "Gas": "generation__gas__twh",
    "Other fossil": "generation__other_fossil__twh",
    "Hydro": "generation__hydro__twh",
    "Nuclear": "generation__nuclear__twh",
    "Solar": "generation__solar__twh",
    "Wind": "generation__wind__twh",
    "Other renewables": "generation__other_renewables__twh",
    "Clean": "generation__clean__twh",
    "Renewables": "generation__renewables__twh",
    "Fossil": "generation__fossil__twh",
    "Total generation": "generation__total_generation__twh",
}


def sanity_check_inputs(tb: Table) -> None:
    sources = set(tb["electricity_source"])
    missing = set(GENERATION_SOURCE_TO_COLUMN) - sources
    assert not missing, f"Missing expected electricity sources: {missing}"
    for extra in ["Demand", "Net imports"]:
        assert extra in sources, f"Missing '{extra}' source."
    assert tb["date"].notna().all(), "Null dates in monthly data."
    # Generation must be non-negative for the actual generation sources. "Net imports" is signed
    # (negative for net exporters) and "Demand" is a separate quantity, so exclude them.
    gen = tb[tb["electricity_source"].isin(GENERATION_SOURCE_TO_COLUMN)]["generation__twh"].dropna()
    assert (gen >= -1e-6).all(), "Negative generation found in monthly data."


def _extract(tb: Table, source: str, columns: dict[str, str]) -> Table:
    """Pull a single source's rows and rename its metric columns."""
    return tb[tb["electricity_source"] == source][["country", "date"] + list(columns)].rename(
        columns=columns, errors="raise"
    )


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("monthly_electricity")
    tb = ds_meadow.read("monthly_electricity", safe_types=False)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb, warn_on_missing_countries=True, warn_on_unused_countries=False)
    tb["electricity_source"] = tb["electricity_source"].astype(str)

    sanity_check_inputs(tb)

    # Keep the Ember origin to reattach after pivoting/merging (those operations drop column-level origins).
    origins = tb["generation__twh"].metadata.origins

    # Generation by source -> one column per source.
    gen = tb[tb["electricity_source"].isin(GENERATION_SOURCE_TO_COLUMN)].copy()
    gen["column"] = gen["electricity_source"].map(GENERATION_SOURCE_TO_COLUMN)
    wide = gen.pivot(index=["country", "date"], columns="column", values="generation__twh", join_column_levels_with="")

    # Demand and net imports carry their value in the generation column of their own source row.
    demand = _extract(tb, "Demand", {"generation__twh": "demand__total_demand__twh"})
    net_imports = _extract(tb, "Net imports", {"generation__twh": "imports__total_net_imports__twh"})
    # Total emissions and overall CO2 intensity live on the "Total generation" row.
    emissions = _extract(
        tb,
        "Total generation",
        {
            "emissions__mtco2e": "emissions__lifecycle__total_emissions__mtco2",
            "emissions_intensity__gco2e_kwh": "emissions__lifecycle__co2_intensity__gco2_kwh",
        },
    )

    tb_wide = wide
    for extra in [demand, net_imports, emissions]:
        tb_wide = pr.merge(tb_wide, extra, on=["country", "date"], how="left")

    # Reattach the Ember origin to every indicator.
    for column in tb_wide.columns:
        if column not in ["country", "date"]:
            tb_wide[column].metadata.origins = origins

    tb_wide = tb_wide.format(keys=["country", "date"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_wide])
    ds_garden.save()
