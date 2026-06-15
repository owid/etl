"""Greenhouse gas emissions by human activity (electricity shown as its own activity).

This step categorizes Climate Watch (WRI) CO2 and greenhouse-gas emissions by the end use of human
activities, keeping electricity generation as its own activity. It produces two country-level tables:

1. ``emissions_by_human_activity``
   Five activities: Growing food, Getting around, Keeping warm and cool, Electricity, Making things.
   Waste, fugitive emissions and other fuel combustion are folded into the activities above.

2. ``emissions_by_human_activity_including_other``
   The same, but those miscellaneous emissions (other fuel combustion + waste + fugitive emissions) are
   broken out into a separate "Other" activity instead of being folded in.

A separate step (``emissions_by_human_activity_splitting_electricity``) loads these tables and
redistributes the Electricity activity into the activities that consume the electricity.

The ideal data for these custom categories comes from the IEA, but it is under a heavy paywall, so the
tables are built from Climate Watch as an approximation. See WRI's methodology:
https://files.wri.org/d8/s3fs-public/2024-05/climate-watch-country-greenhouse-gas-emissions-data-methodology.pdf?VersionId=1geU96keSmqZlUjv41FNGB4CLpHQbruN
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Climate Watch subsectors grouped into custom activities, with the miscellaneous emissions folded in
# (other fuel combustion into "Growing food", and waste and fugitive emissions into "Making things").
SECTOR_MAPPING = {
    "Growing food": ["agriculture", "land_use_change_and_forestry", "other_fuel_combustion"],
    "Getting around": ["transport"],
    "Keeping warm and cool": ["buildings"],
    "Electricity": ["electricity_and_heat"],
    "Making things": ["manufacturing_and_construction", "industry", "fugitive", "waste"],
}

# Same grouping, but with other fuel combustion, waste and fugitive emissions shown as a separate "Other" activity.
SECTOR_MAPPING_INCLUDING_OTHER = {
    "Growing food": ["agriculture", "land_use_change_and_forestry"],
    "Getting around": ["transport"],
    "Keeping warm and cool": ["buildings"],
    "Electricity": ["electricity_and_heat"],
    "Making things": ["manufacturing_and_construction", "industry"],
    "Other": ["other_fuel_combustion", "waste", "fugitive"],
}

# All Climate Watch subsectors expected in the data.
EXPECTED_SECTORS = sorted(set(sum(SECTOR_MAPPING.values(), [])))

# Climate Watch's CO2 table has no data for these subsectors (their CO2 emissions are negligible).
SECTORS_WITHOUT_CO2 = ["agriculture", "waste"]


def sanity_check_inputs(tb_ghg, tb_co2):
    error = "Unexpected list of sectors in the GHG table."
    assert set(EXPECTED_SECTORS) <= set(tb_ghg.columns), error
    error = "CO2 table should contain all sectors except agriculture and waste."
    assert set(EXPECTED_SECTORS) - set(tb_co2.columns) == set(SECTORS_WITHOUT_CO2), error

    # At World level, the sum of subsector emissions should match the reported total within a few percent.
    # NOTE: This is checked only for World; at country level the source totals are noisier.
    for gas, _tb in [("ghg", tb_ghg), ("co2", tb_co2)]:
        _world = _tb[_tb["country"] == "World"].copy()
        _cols = [c for c in EXPECTED_SECTORS if c in _tb.columns]
        _world["sum"] = _world[_cols].sum(axis=1)
        error = f"Sum of {gas} subsector emissions differs from the reported total by more than a few percent."
        assert (
            (100 * abs(_world["sum"] - _world["total_including_lucf"]) / _world["total_including_lucf"]) < 4
        ).all(), error


def build_table(tb_co2, tb_ghg, mapping, short_name):
    # Work on copies, since each table groups the same subsectors differently.
    tb_co2 = tb_co2.copy()
    tb_ghg = tb_ghg.copy()

    # Aggregate Climate Watch subsectors into our custom activities.
    for sector, subsectors in mapping.items():
        tb_co2[sector] = tb_co2[subsectors].sum(axis=1)
        tb_ghg[sector] = tb_ghg[subsectors].sum(axis=1)
    tb_co2 = tb_co2[["country", "year"] + list(mapping)]
    tb_ghg = tb_ghg[["country", "year"] + list(mapping)]

    # For convenience, transpose tables and combine them.
    tb_co2 = tb_co2.melt(id_vars=["country", "year"], var_name="sector", value_name="co2_emissions")
    tb_ghg = tb_ghg.melt(id_vars=["country", "year"], var_name="sector", value_name="ghg_emissions")
    tb = tb_ghg.merge(tb_co2, on=["country", "year", "sector"], how="outer")

    # Add an explanation of which Climate Watch sectors make up each activity.
    description_processing = "Each activity is built from the following Climate Watch sectors:"
    for sector, subsectors in mapping.items():
        names = ", ".join(s.replace("fugitive", "fugitive emissions").replace("_", " ") for s in subsectors)
        description_processing += f"\n- {sector}: {names}."
    for column in ["co2_emissions", "ghg_emissions"]:
        tb[column].metadata.description_processing = description_processing

    # Improve table format.
    tb = tb.format(keys=["country", "year", "sector"], short_name=short_name)

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load Climate Watch's emissions by sector and read its CO2 and GHG tables.
    ds = paths.load_dataset("emissions_by_sector")
    tb_co2 = ds.read("carbon_dioxide_emissions_by_sector")
    tb_ghg = ds.read("greenhouse_gas_emissions_by_sector")

    #
    # Process data.
    #
    # Rename columns in both tables, for convenience.
    tb_co2 = tb_co2.rename(
        columns={column: column.replace("_co2_emissions", "") for column in tb_co2.columns}, errors="raise"
    )
    tb_ghg = tb_ghg.rename(
        columns={column: column.replace("_ghg_emissions", "") for column in tb_ghg.columns}, errors="raise"
    )

    # Sanity checks.
    sanity_check_inputs(tb_ghg=tb_ghg, tb_co2=tb_co2)

    # The CO2 table has no agriculture or waste data; add them as zeros so the mappings can sum them.
    for sector in SECTORS_WITHOUT_CO2:
        tb_co2[sector] = 0

    # Keep only columns of subsector emissions.
    tb_co2 = tb_co2[["country", "year"] + EXPECTED_SECTORS]
    tb_ghg = tb_ghg[["country", "year"] + EXPECTED_SECTORS]

    # Build both tables (electricity is its own activity in both).
    tb = build_table(tb_co2, tb_ghg, SECTOR_MAPPING, "emissions_by_human_activity")
    tb_other = build_table(
        tb_co2, tb_ghg, SECTOR_MAPPING_INCLUDING_OTHER, "emissions_by_human_activity_including_other"
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_other])
    ds_garden.save()
