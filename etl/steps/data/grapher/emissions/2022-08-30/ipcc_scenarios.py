"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.files import yaml_load
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

TWH_TO_KWH = 1e9
TON_TO_KG = 1e3

UNITS_CONVERSION = {
    "EJ/yr": 277.778,  # 1 EJ = 277.778 TWh
    "Mt CO2/yr": 1e6,
    "Mt CH4/yr": 1e6,
    "Mt CO2-equiv/yr": 1e6,
    "Mt BC/yr": 1e6,
    "Mt CO/yr": 1e6,
    "Mt NH3/yr": 1e6,
    "Mt NO2/yr": 1e6,
    "Mt OC/yr": 1e6,
    "Mt SO2/yr": 1e6,
    "Mt VOC/yr": 1e6,
    "kt N2O/yr": 1e3,
    "billion US$2005/yr": 1e9,
    "million": 1e6,
    "GW": 1,
    "million t DM/yr": 1e6,
    "million ha": 1e6,
    "W/m2": 1,
    "US$2005/t CO2": 1,
    "bn tkm/yr": 1e9,
    "bn pkm/yr": 1e9,
    "ppb": 1,
    "ppm": 1,
    "Â°C": 1,
}


def process_region(combined: Table, naming: Table, scenario_naming: Table, annotations: Table, region=None):
    """Process data for a specific region."""
    # Filter data by region if specified
    if region:
        tb = combined[combined["REGION"] == region]
    else:
        tb = combined[combined["REGION"] == "World"]

    # Common processing steps
    tb = tb.drop(columns=["REGION"])
    tb = tb.melt(id_vars=["SCENARIO", "VARIABLE", "UNIT"], var_name="Year", value_name="Value")
    tb = pr.merge(tb, naming, on="VARIABLE", how="inner")
    tb = tb.drop(columns=["VARIABLE"])

    # Convert units
    tb.Value *= tb.UNIT.map(UNITS_CONVERSION).astype(float)
    tb = tb.drop(columns=["UNIT"])

    tb = tb.pivot_table(index=["SCENARIO", "Year"], columns="Variable", values="Value", aggfunc="first").reset_index()
    tb = pr.merge(tb, scenario_naming, on="SCENARIO", how="inner")
    tb = tb.drop(columns=["SCENARIO"])

    # Use units to avoid errors
    # NOTE: in the next update, use units explicitly everywhere!
    tb["emissions_co2_kg"] = tb["emissions_co2"] * TON_TO_KG
    tb["final_energy_kwh"] = tb["final_energy"] * TWH_TO_KWH
    tb["primary_energy_kwh"] = tb["primary_energy"] * TWH_TO_KWH

    # Calculate derived metrics
    tb["carbon_intensity_economy"] = tb["emissions_co2_kg"] / tb["gdp"]
    tb["carbon_intensity_energy"] = tb["emissions_co2_kg"] / tb["primary_energy_kwh"]
    tb["primary_energy_intensity"] = tb["primary_energy_kwh"] / tb["gdp"]
    tb["final_energy_intensity"] = tb["final_energy_kwh"] / tb["gdp"]
    tb["primary_final_efficiency"] = tb["final_energy_kwh"] / tb["primary_energy_kwh"] * 100

    # Calculate share of energy
    for col in tb.columns:
        # TODO: unify inconsistent naming
        if col.startswith("secondary_energy_"):
            new_col = col + "_share"
            tb[new_col] = tb[col] / tb["secondary_energy_elec"] * 100
        elif col.startswith("primary_energy_"):
            new_col = "primary_energy_share_" + col.replace("primary_energy_", "")
            tb[new_col] = tb[col] / tb["primary_energy"] * 100

    # Rename some columns
    # TODO: this is for backward compatibility, remove in the future
    tb = tb.rename(
        columns={
            "econ_consumption": "econ_consumption_dollars",
            "final_energy_intensity": "energy_intensity_final",
            "primary_energy_intensity": "energy_intensity_primary",
        }
    )

    # Convert TWh to kWh
    tb["final_elec_kwh"] = tb["final_energy_elec"] * TWH_TO_KWH
    tb["secondary_energy_elec_kwh"] = tb["secondary_energy_elec"] * TWH_TO_KWH
    for col in tb.columns:
        if any(col.startswith(prefix) for prefix in ("final_energy", "secondary_energy", "primary_energy")):
            new_col = col + "_kwh"
            tb[new_col] = tb[col] * TWH_TO_KWH

    # Calculate per capita metrics
    tb["gdp_per_capita"] = tb["gdp"] / tb["population"]
    tb["econ_consumption_per_capita"] = tb["econ_consumption_dollars"] / tb["population"]
    tb["co2_per_capita"] = tb["emissions_co2"] / tb["population"]
    tb["ch4_per_capita"] = tb["emissions_ch4"] / tb["population"]
    tb["n2o_per_capita"] = tb["emissions_n2o"] / tb["population"]
    tb["final_energy_per_capita"] = tb["final_energy_kwh"] / tb["population"]
    tb["electricity_per_capita"] = tb["final_elec_kwh"] / tb["population"]
    tb["heat_per_capita"] = tb["final_energy_heat_kwh"] / tb["population"]
    tb["industry_per_capita"] = tb["final_energy_industry_kwh"] / tb["population"]
    tb["transport_per_capita"] = tb["final_energy_transport_kwh"] / tb["population"]
    tb["secondary_elec_per_capita"] = tb["secondary_energy_elec_kwh"] / tb["population"]
    tb["secondary_heat_per_capita"] = tb["secondary_energy_heat_kwh"] / tb["population"]
    tb["primary_energy_per_capita"] = tb["primary_energy_kwh"] / tb["population"]

    # Add annotations and reorder columns
    tb = pr.merge(tb, annotations, on="Entity")

    return tb


def process(snap):
    """Process IPCC scenario data from archive."""
    # Read input files from archive
    combined = snap.read_from_archive("ipcc.csv", encoding="latin1")
    combined = combined.drop(columns=["MODEL"])

    naming = snap.read_from_archive("variable_naming.csv", encoding="latin1")
    scenario_naming = snap.read_from_archive("scenario_naming.csv", encoding="latin1")
    annotations = snap.read_from_archive("annotations.csv", encoding="latin1")

    # Process each region
    region_mapping = {
        None: ("IPCC Scenarios (IIASA).csv", "global"),
        "R5.2ASIA": ("IPCC Scenarios - Asia (IIASA).csv", "asia"),
        "R5.2MAF": ("IPCC Scenarios - Middle East and Africa (IIASA).csv", "middle_east_and_africa"),
        "R5.2LAM": ("IPCC Scenarios - Latin America (IIASA).csv", "latin_america"),
        "R5.2OECD": ("IPCC Scenarios - OECD (IIASA).csv", "oecd"),
        # "R5.2REF": ("IPCC Scenarios - Reforming economies (IIASA).csv", 'ref'),
    }

    tables = []
    for region, (output_name, table_name) in region_mapping.items():
        tb = process_region(combined, naming, scenario_naming, annotations, region)

        tb["region"] = table_name

        # Add origin
        for col in tb.columns:
            tb[col].m.origins = [snap.m.origin]

        tables.append(tb)

    # Concat all tables
    tb = pr.concat(tables, axis=0)

    # Underscore table
    tb = tb.underscore()

    # Set index
    tb = tb.rename(columns={"entity": "country"}).set_index(["region", "country", "year"])

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ipcc_scenarios.zip")

    # Process data from snapshot
    with snap.open_archive():
        tb = process(snap)

    tb.m.short_name = "ipcc_scenarios"

    # Drop all columns that are not in the YAML file
    # TODO: ideally we have metadata for all columns
    with open(paths.metadata_path, "r") as f:
        meta = yaml_load(f)
        use_cols = [c for c in meta["tables"]["ipcc_scenarios"]["variables"]]
        tb = tb[use_cols]

    #
    # Save outputs.
    #
    # Create a new grapher dataset
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
