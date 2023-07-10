"""Generate BP energy mix dataset using data from BP's statistical review of the world energy.

"""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_population

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Exajoules to terawatt-hours.
EJ_TO_TWH = 1e6 / 3600
# Petajoules to exajoules.
PJ_TO_EJ = 1e-3

# List all energy sources in the data.
ONLY_DIRECT_ENERGY = ["Coal", "Fossil fuels", "Gas", "Oil", "Biofuels"]
DIRECT_AND_EQUIVALENT_ENERGY = [
    "Hydro",
    "Low-carbon energy",
    "Nuclear",
    "Other renewables",
    "Renewables",
    "Solar",
    "Wind",
    "Solar and wind",
]
ALL_SOURCES = sorted(ONLY_DIRECT_ENERGY + DIRECT_AND_EQUIVALENT_ENERGY)


def get_bp_data(bp_table: Table) -> pd.DataFrame:
    """Extract a simple dataframe of BP statistical review data from the table in the dataset.

    Parameters
    ----------
    bp_table : Table
        BP table (from the dataset of BP statistical review).

    Returns
    -------
    bp_data : pd.DataFrame
        BP statistical review data.

    """
    bp_table = bp_table.copy()

    # Convert table (snake case) column names to human readable names.
    bp_table = bp_table.rename(
        columns={column: bp_table[column].metadata.title for column in bp_table.columns if column != "country_code"}
    ).reset_index()

    # Rename human-readable columns (and select only the ones that will be used).
    columns = {
        "country": "Country",
        "country_code": "Country code",
        "year": "Year",
        # Fossil fuel primary energy (in EJ).
        "Coal Consumption - EJ": "Coal (EJ)",
        "Gas Consumption - EJ": "Gas (EJ)",
        "Oil Consumption - EJ": "Oil (EJ)",
        # Non-fossil based electricity generation (in TWh).
        "Hydro Generation - TWh": "Hydro (TWh - direct)",
        "Nuclear Generation - TWh": "Nuclear (TWh - direct)",
        "Solar Generation - TWh": "Solar (TWh - direct)",
        "Wind Generation - TWh": "Wind (TWh - direct)",
        "Geo Biomass Other - TWh": "Other renewables (TWh - direct)",
        # Non-fossil based electricity generation converted into input-equivalent primary energy (in EJ).
        "Hydro Consumption - EJ": "Hydro (EJ - equivalent)",
        "Nuclear Consumption - EJ": "Nuclear (EJ - equivalent)",
        "Solar Consumption - EJ": "Solar (EJ - equivalent)",
        "Wind Consumption - EJ": "Wind (EJ - equivalent)",
        "Geo Biomass Other - EJ": "Other renewables (EJ - equivalent)",
        # Total, input-equivalent primary energy consumption (in EJ).
        "Primary Energy Consumption - EJ": "Primary energy (EJ - equivalent)",
        # Biofuels consumption (in PJ, that will be converted into EJ).
        "Biofuels Consumption - PJ - Total": "Biofuels (PJ)",
    }

    # Create a simple dataframe (without metadata and with a dummy index).
    assert set(columns) < set(bp_table.columns), "Column names have changed in BP data."

    bp_data = pd.DataFrame(bp_table)[list(columns)].rename(errors="raise", columns=columns)

    return bp_data


def _check_that_substitution_method_is_well_calculated(
    primary_energy: pd.DataFrame,
) -> None:
    # Check that the constructed primary energy using the substitution method (in TWh) coincides with the
    # input-equivalent primary energy (converted from EJ into TWh) given in the original data.
    check = primary_energy[
        [
            "Year",
            "Country",
            "Primary energy (EJ - equivalent)",
            "Primary energy (TWh - equivalent)",
        ]
    ].reset_index(drop=True)
    check["Primary energy (TWh - equivalent) - original"] = check["Primary energy (EJ - equivalent)"] * EJ_TO_TWH
    check = check.dropna().reset_index(drop=True)
    # They may not coincide exactly, but at least check that they differ (point by point) by less than 10%.
    max_deviation = max(
        abs(
            (check["Primary energy (TWh - equivalent)"] - check["Primary energy (TWh - equivalent) - original"])
            / check["Primary energy (TWh - equivalent) - original"]
        )
    )
    assert max_deviation < 0.1


def calculate_direct_primary_energy(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Convert direct primary energy into TWh and create various aggregates (e.g. Fossil fuels and Renewables).

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        Data, after adding direct primary energy.

    """
    primary_energy = primary_energy.copy()

    # Convert units of biofuels consumption.
    primary_energy["Biofuels (EJ)"] = primary_energy["Biofuels (PJ)"] * PJ_TO_EJ

    # Create column for fossil fuels primary energy (if any of them is nan, the sum will be nan).
    primary_energy["Fossil fuels (EJ)"] = (
        primary_energy["Coal (EJ)"] + primary_energy["Oil (EJ)"] + primary_energy["Gas (EJ)"]
    )

    # Convert primary energy of fossil fuels and biofuels into TWh.
    for cat in ["Coal", "Oil", "Gas", "Biofuels"]:
        primary_energy[f"{cat} (TWh)"] = primary_energy[f"{cat} (EJ)"] * EJ_TO_TWH

    # Create column for primary energy from fossil fuels (in TWh).
    primary_energy["Fossil fuels (TWh)"] = (
        primary_energy["Coal (TWh)"] + primary_energy["Oil (TWh)"] + primary_energy["Gas (TWh)"]
    )

    # Create column for direct primary energy from renewable sources in TWh.
    # (total renewable electricity generation and biofuels) (in TWh).
    # By visually inspecting the original data, it seems that many data points that used to be zero are
    # missing in the 2022 release, so filling nan with zeros seems to be a reasonable approach to avoids losing a
    # significant amount of data.
    primary_energy["Renewables (TWh - direct)"] = (
        primary_energy["Hydro (TWh - direct)"]
        + primary_energy["Solar (TWh - direct)"].fillna(0)
        + primary_energy["Wind (TWh - direct)"].fillna(0)
        + primary_energy["Other renewables (TWh - direct)"].fillna(0)
        + primary_energy["Biofuels (TWh)"].fillna(0)
    )
    # Create column for direct primary energy from low-carbon sources in TWh.
    # (total renewable electricity generation, biofuels, and nuclear power) (in TWh).
    primary_energy["Low-carbon energy (TWh - direct)"] = primary_energy["Renewables (TWh - direct)"] + primary_energy[
        "Nuclear (TWh - direct)"
    ].fillna(0)
    # Create column for direct primary energy from solar and wind in TWh.
    primary_energy["Solar and wind (TWh - direct)"] = primary_energy["Solar (TWh - direct)"].fillna(0) + primary_energy[
        "Wind (TWh - direct)"
    ].fillna(0)
    # Create column for total direct primary energy.
    primary_energy["Primary energy (TWh - direct)"] = (
        primary_energy["Fossil fuels (TWh)"] + primary_energy["Low-carbon energy (TWh - direct)"]
    )

    return primary_energy


def calculate_equivalent_primary_energy(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Convert input-equivalent primary energy into TWh and create various aggregates (e.g. Fossil fuels and
    Renewables).

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        Data, after adding input-equivalent primary energy.

    """
    primary_energy = primary_energy.copy()
    # Create column for total renewable input-equivalent primary energy (in EJ).
    # Fill missing values with zeros (see comment above).
    primary_energy["Renewables (EJ - equivalent)"] = (
        primary_energy["Hydro (EJ - equivalent)"]
        + primary_energy["Solar (EJ - equivalent)"].fillna(0)
        + primary_energy["Wind (EJ - equivalent)"].fillna(0)
        + primary_energy["Other renewables (EJ - equivalent)"].fillna(0)
        + primary_energy["Biofuels (EJ)"].fillna(0)
    )
    # Create column for low carbon energy (i.e. renewable plus nuclear energy).
    primary_energy["Low-carbon energy (EJ - equivalent)"] = primary_energy[
        "Renewables (EJ - equivalent)"
    ] + primary_energy["Nuclear (EJ - equivalent)"].fillna(0)
    # Create column for solar and wind.
    primary_energy["Solar and wind (EJ - equivalent)"] = primary_energy["Solar (EJ - equivalent)"].fillna(
        0
    ) + primary_energy["Wind (EJ - equivalent)"].fillna(0)
    # Convert input-equivalent primary energy of non-fossil based electricity into TWh.
    # The result is primary energy using the "substitution method".
    for cat in DIRECT_AND_EQUIVALENT_ENERGY:
        primary_energy[f"{cat} (TWh - equivalent)"] = primary_energy[f"{cat} (EJ - equivalent)"] * EJ_TO_TWH
    # Create column for primary energy from all sources (which corresponds to input-equivalent primary
    # energy for non-fossil based sources).
    primary_energy["Primary energy (TWh - equivalent)"] = (
        primary_energy["Fossil fuels (TWh)"] + primary_energy["Low-carbon energy (TWh - equivalent)"]
    )
    # Check that the primary energy constructed using the substitution method coincides with the
    # input-equivalent primary energy.
    _check_that_substitution_method_is_well_calculated(primary_energy)

    return primary_energy


def calculate_share_of_primary_energy(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Calculate the share (percentage) of (direct or direct and input-equivalent) primary energy for each energy
     source.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding columns for the share of primary energy.

    """
    primary_energy = primary_energy.copy()
    # Check that all sources are included in the data.
    expected_sources = sorted(
        set(
            [
                source.split("(")[0].strip()
                for source in primary_energy.columns
                if not source.startswith(("Country", "Year", "Primary"))
            ]
        )
    )
    assert expected_sources == ALL_SOURCES, "Sources may have changed names."

    for source in ONLY_DIRECT_ENERGY:
        # Calculate each source as share of direct primary energy.
        primary_energy[f"{source} (% direct primary energy)"] = (
            primary_energy[f"{source} (TWh)"] / primary_energy["Primary energy (TWh - direct)"] * 100
        )
        # Calculate each source as share of input-equivalent primary energy (i.e. substitution method).
        primary_energy[f"{source} (% equivalent primary energy)"] = (
            primary_energy[f"{source} (EJ)"] / primary_energy["Primary energy (EJ - equivalent)"] * 100
        )

    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        # Calculate each source as share of direct primary energy.
        primary_energy[f"{source} (% direct primary energy)"] = (
            primary_energy[f"{source} (TWh - direct)"] / primary_energy["Primary energy (TWh - direct)"] * 100
        )
        # Calculate each source as share of input-equivalent primary energy (i.e. substitution method).
        primary_energy[f"{source} (% equivalent primary energy)"] = (
            primary_energy[f"{source} (EJ - equivalent)"] / primary_energy["Primary energy (EJ - equivalent)"] * 100
        )

    return primary_energy


def calculate_primary_energy_annual_change(
    primary_energy: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate annual change of (direct or direct and input-equivalent) primary energy for each energy source.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding annual changes.

    """
    primary_energy = primary_energy.copy()

    # Calculate annual change in each source.
    primary_energy = primary_energy.sort_values(["Country", "Year"]).reset_index(drop=True)
    for source in ONLY_DIRECT_ENERGY:
        # Create column for source percentage growth as a function of direct primary energy.
        primary_energy[f"{source} (% growth)"] = primary_energy.groupby("Country")[f"{source} (TWh)"].pct_change() * 100
        # Create column for source absolute growth as a function of direct primary energy.
        primary_energy[f"{source} (TWh growth)"] = primary_energy.groupby("Country")[f"{source} (TWh)"].diff()

    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        # Create column for source percentage growth as a function of primary energy
        # (as a percentage, it is irrelevant whether it is direct or equivalent).
        primary_energy[f"{source} (% growth)"] = (
            primary_energy.groupby("Country")[f"{source} (TWh - direct)"].pct_change() * 100
        )
        # Create column for source absolute growth as a function of direct primary energy.
        primary_energy[f"{source} (TWh growth - direct)"] = primary_energy.groupby("Country")[
            f"{source} (TWh - direct)"
        ].diff()
        # Create column for source absolute growth as a function of input-equivalent primary energy.
        primary_energy[f"{source} (TWh growth - equivalent)"] = primary_energy.groupby("Country")[
            f"{source} (TWh - equivalent)"
        ].diff()

    return primary_energy


def add_per_capita_variables(primary_energy: pd.DataFrame, df_population: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita variables.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.
    df_population : pd.DataFrame
        Population data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding per-capita variables.

    """
    primary_energy = primary_energy.copy()

    primary_energy = add_population(
        df=primary_energy,
        population=df_population,
        country_col="Country",
        year_col="Year",
        population_col="Population",
        warn_on_missing_countries=False,
    )
    for source in ONLY_DIRECT_ENERGY:
        primary_energy[f"{source} per capita (kWh)"] = (
            primary_energy[f"{source} (TWh)"] / primary_energy["Population"] * TWH_TO_KWH
        )
    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        primary_energy[f"{source} per capita (kWh - direct)"] = (
            primary_energy[f"{source} (TWh - direct)"] / primary_energy["Population"] * TWH_TO_KWH
        )
        primary_energy[f"{source} per capita (kWh - equivalent)"] = (
            primary_energy[f"{source} (TWh - equivalent)"] / primary_energy["Population"] * TWH_TO_KWH
        )

    # Drop unnecessary column.
    primary_energy = primary_energy.drop(columns=["Population"])

    return primary_energy


def prepare_output_table(primary_energy: pd.DataFrame) -> Table:
    """Create a table with the processed data, ready to be in a garden dataset and to be uploaded to grapher (although
    additional metadata may need to be added to the table).

    Parameters
    ----------
    primary_energy : pd.DataFrame
        Processed BP data.

    Returns
    -------
    table : catalog.Table
        Table, ready to be added to a new garden dataset.

    """
    # Keep only columns in TWh (and not EJ or PJ).
    table = Table(primary_energy, short_name="energy_mix").drop(
        errors="raise",
        columns=[column for column in primary_energy.columns if (("(EJ" in column) or ("(PJ" in column))],
    )

    # Replace spurious inf values by nan.
    table = table.replace([np.inf, -np.inf], np.nan)

    # Sort conveniently and add an index.
    table = (
        table.sort_values(["Country", "Year"])
        .reset_index(drop=True)
        .set_index(["Country", "Year"], verify_integrity=True)
        .astype({"Country code": "category"})
    )

    # Add metadata (e.g. unit) to each column.
    # Define unit names (these are the long and short unit names that will be shown in grapher).
    # The keys of the dictionary should correspond to units expected to be found in each of the variable names in table.
    short_unit_to_unit = {
        "TWh": "terawatt-hours",
        "kWh": "kilowatt-hours",
        "%": "%",
    }
    # Define number of decimal places to show (only relevant for grapher, not for the data).
    short_unit_to_num_decimals = {
        "TWh": 0,
        "kWh": 0,
    }
    for column in table.columns:
        table[column].metadata.title = column
        for short_unit in ["TWh", "kWh", "%"]:
            if short_unit in column:
                table[column].metadata.short_unit = short_unit
                table[column].metadata.unit = short_unit_to_unit[short_unit]
                table[column].metadata.display = {}
                if short_unit in short_unit_to_num_decimals:
                    table[column].metadata.display["numDecimalPlaces"] = short_unit_to_num_decimals[short_unit]
                # Add the variable name without unit (only relevant for grapher).
                table[column].metadata.display["name"] = column.split(" (")[0]

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the latest BP statistical review.
    ds_bp: Dataset = paths.load_dependency("statistical_review")
    tb_bp = ds_bp["statistical_review"]

    # Load the population dataset.
    ds_population: Dataset = paths.load_dependency("population")
    # Get table from dataset.
    tb_population = ds_population["population"]
    # Make a dataframe out of the data in the table, with the required columns.
    df_population = pd.DataFrame(tb_population)

    #
    # Process data.
    #
    # Get a dataframe out of the BP table.
    primary_energy = get_bp_data(bp_table=tb_bp)

    # Calculate direct and primary energy using the substitution method.
    primary_energy = calculate_direct_primary_energy(primary_energy=primary_energy)
    primary_energy = calculate_equivalent_primary_energy(primary_energy=primary_energy)

    # Calculate share of (direct and sub-method) primary energy.
    primary_energy = calculate_share_of_primary_energy(primary_energy=primary_energy)

    # Calculate annual change of primary energy.
    primary_energy = calculate_primary_energy_annual_change(primary_energy)

    # Add per-capita variables.
    primary_energy = add_per_capita_variables(primary_energy=primary_energy, df_population=df_population)

    # Prepare output data in a convenient way.
    table = prepare_output_table(primary_energy)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[table], default_metadata=ds_bp.metadata)
    ds_garden.save()
