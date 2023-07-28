"""Generate the energy mix dataset using data from the Energy Institute Statistical Review of World Energy.

"""

import numpy as np
from owid.catalog import Dataset, Table

from etl.data_helpers.geo import add_population_to_table
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


def get_statistical_review_data(tb_review: Table) -> Table:
    """Select necessary data from the Statistical Review.

    Parameters
    ----------
    tb_review : Table
        Statistical Review table.

    Returns
    -------
    tb_review : Table
        Selected data.

    """
    tb_review = tb_review.copy()

    # Convert table (snake case) column names to human readable names.
    tb_review = tb_review.rename(
        columns={column: tb_review[column].metadata.title for column in tb_review.columns if column != "country_code"}
    )

    # Rename human-readable columns (and select only the ones that will be used).
    columns = {
        # Fossil fuel primary energy.
        "Coal consumption - TWh": "Coal (TWh)",
        "Gas consumption - TWh": "Gas (TWh)",
        "Oil consumption - TWh": "Oil (TWh)",
        # Non-fossil based electricity generation.
        # NOTE: We created estimates of the "direct" primary energy consumption in the Statistical Review garden step,
        # but instead we will use the electricity generation.
        "Hydro generation - TWh": "Hydro (TWh - direct)",
        "Nuclear generation - TWh": "Nuclear (TWh - direct)",
        "Solar generation - TWh": "Solar (TWh - direct)",
        "Wind generation - TWh": "Wind (TWh - direct)",
        "Other renewables (including geothermal and biomass) electricity generation - TWh": "Other renewables (TWh - direct)",
        # Non-fossil based electricity generation converted into input-equivalent primary energy.
        "Hydro consumption - TWh": "Hydro (TWh - equivalent)",
        "Nuclear consumption - TWh": "Nuclear (TWh - equivalent)",
        "Solar consumption - TWh": "Solar (TWh - equivalent)",
        "Wind consumption - TWh": "Wind (TWh - equivalent)",
        "Other renewables (including geothermal and biomass) - TWh": "Other renewables (TWh - equivalent)",
        # Total, input-equivalent primary energy consumption.
        # NOTE: The input-equivalent primary energy consumption will be calculated later on, so the following column
        # will be used just to sanity check.
        "Primary energy consumption - TWh": "Primary energy (TWh - equivalent) - original",
        # Biofuels consumption.
        "Biofuels consumption - TWh": "Biofuels (TWh)",
    }

    # Sanity check.
    assert set(columns) < set(tb_review.columns), "Column names have changed in Statistical Review data."

    # Select and rename columns.
    tb_review = tb_review[list(columns)].rename(errors="raise", columns=columns)

    return tb_review


def calculate_direct_primary_energy(primary_energy: Table) -> Table:
    """Convert direct primary energy into TWh and create various aggregates (e.g. Fossil fuels and Renewables).

    Parameters
    ----------
    primary_energy : Table
        Primary energy data from the Statistical Review.

    Returns
    -------
    primary_energy : Table
        Data, after adding direct primary energy.

    """
    primary_energy = primary_energy.copy()

    # Create column for primary energy from fossil fuels.
    primary_energy["Fossil fuels (TWh)"] = (
        primary_energy["Coal (TWh)"] + primary_energy["Oil (TWh)"] + primary_energy["Gas (TWh)"]
    )

    # Create column for direct primary energy from renewable sources.
    # (total renewable electricity generation and biofuels).
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
    # Create column for direct primary energy from low-carbon sources.
    # (total renewable electricity generation, biofuels, and nuclear power).
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


def calculate_equivalent_primary_energy(primary_energy: Table) -> Table:
    """Convert input-equivalent primary energy into TWh and create various aggregates (e.g. Fossil fuels and
    Renewables).

    Parameters
    ----------
    primary_energy : Table
        Primary energy data.

    Returns
    -------
    primary_energy : Table
        Data, after adding input-equivalent primary energy.

    """
    primary_energy = primary_energy.copy()

    # Create column for total renewable input-equivalent primary energy.
    # Fill missing values with zeros (see comment above).
    primary_energy["Renewables (TWh - equivalent)"] = (
        primary_energy["Hydro (TWh - equivalent)"]
        + primary_energy["Solar (TWh - equivalent)"].fillna(0)
        + primary_energy["Wind (TWh - equivalent)"].fillna(0)
        + primary_energy["Other renewables (TWh - equivalent)"].fillna(0)
        + primary_energy["Biofuels (TWh)"].fillna(0)
    )
    # Create column for low carbon energy (i.e. renewable plus nuclear energy).
    primary_energy["Low-carbon energy (TWh - equivalent)"] = primary_energy[
        "Renewables (TWh - equivalent)"
    ] + primary_energy["Nuclear (TWh - equivalent)"].fillna(0)
    # Create column for solar and wind.
    primary_energy["Solar and wind (TWh - equivalent)"] = primary_energy["Solar (TWh - equivalent)"].fillna(
        0
    ) + primary_energy["Wind (TWh - equivalent)"].fillna(0)
    # Create column for primary energy from all sources (which corresponds to input-equivalent primary
    # energy for non-fossil based sources).
    # This column should be similar to the original column of primary energy in input-equivalents.
    primary_energy["Primary energy (TWh - equivalent)"] = (
        primary_energy["Fossil fuels (TWh)"] + primary_energy["Low-carbon energy (TWh - equivalent)"]
    )
    # Check that the primary energy constructed using the substitution method coincides with the
    # input-equivalent primary energy.
    _check_that_substitution_method_is_well_calculated(primary_energy)
    # Remove original primary energy column.
    primary_energy = primary_energy.drop(columns=["Primary energy (TWh - equivalent) - original"])

    return primary_energy


def _check_that_substitution_method_is_well_calculated(
    primary_energy: Table,
) -> None:
    # Check that the constructed primary energy using the substitution method (in TWh) coincides with the
    # input-equivalent primary energy (converted from EJ into TWh) given in the original data.
    check = primary_energy[
        [
            "Primary energy (TWh - equivalent) - original",
            "Primary energy (TWh - equivalent)",
        ]
    ]
    check = check.dropna().reset_index(drop=True)
    # They may not coincide exactly, but at least check that they differ (point by point) by less than 10%.
    max_deviation = max(
        abs(
            (check["Primary energy (TWh - equivalent)"] - check["Primary energy (TWh - equivalent) - original"])
            / check["Primary energy (TWh - equivalent) - original"]
        )
    )
    assert max_deviation < 0.1


def calculate_share_of_primary_energy(primary_energy: Table) -> Table:
    """Calculate the share (percentage) of (direct or direct and input-equivalent) primary energy for each energy
     source.

    Parameters
    ----------
    primary_energy : Table
        Primary energy data.

    Returns
    -------
    primary_energy : Table
        Data after adding columns for the share of primary energy.

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
            primary_energy[f"{source} (TWh)"] / primary_energy["Primary energy (TWh - equivalent)"] * 100
        )

    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        # Calculate each source as share of direct primary energy.
        primary_energy[f"{source} (% direct primary energy)"] = (
            primary_energy[f"{source} (TWh - direct)"] / primary_energy["Primary energy (TWh - direct)"] * 100
        )
        # Calculate each source as share of input-equivalent primary energy (i.e. substitution method).
        primary_energy[f"{source} (% equivalent primary energy)"] = (
            primary_energy[f"{source} (TWh - equivalent)"] / primary_energy["Primary energy (TWh - equivalent)"] * 100
        )

    return primary_energy


def calculate_primary_energy_annual_change(
    primary_energy: Table,
) -> Table:
    """Calculate annual change of (direct or direct and input-equivalent) primary energy for each energy source.

    Parameters
    ----------
    primary_energy : Table
        Primary energy data.

    Returns
    -------
    primary_energy : Table
        Data after adding annual changes.

    """
    primary_energy = primary_energy.reset_index()

    # Calculate annual change in each source.
    primary_energy = primary_energy.sort_values(["country", "year"]).reset_index(drop=True)
    for source in ONLY_DIRECT_ENERGY:
        # Create column for source percentage growth as a function of direct primary energy.
        primary_energy[f"{source} (% growth)"] = primary_energy.groupby("country")[f"{source} (TWh)"].pct_change() * 100
        # NOTE: The previous operations do not propagate metadata properly, hence do it manually.
        primary_energy[f"{source} (% growth)"].metadata.sources = primary_energy[f"{source} (TWh)"].metadata.sources
        primary_energy[f"{source} (% growth)"].metadata.licenses = primary_energy[f"{source} (TWh)"].metadata.licenses

        # Create column for source absolute growth as a function of direct primary energy.
        primary_energy[f"{source} (TWh growth)"] = primary_energy.groupby("country")[f"{source} (TWh)"].diff()

    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        # Create column for source percentage growth as a function of primary energy
        # (as a percentage, it is irrelevant whether it is direct or equivalent).
        primary_energy[f"{source} (% growth)"] = (
            primary_energy.groupby("country")[f"{source} (TWh - direct)"].pct_change() * 100
        )
        # NOTE: The previous operations do not propagate metadata properly, hence do it manually.
        primary_energy[f"{source} (% growth)"].metadata.sources = primary_energy[
            f"{source} (TWh - direct)"
        ].metadata.sources
        primary_energy[f"{source} (% growth)"].metadata.licenses = primary_energy[
            f"{source} (TWh - direct)"
        ].metadata.licenses

        # Create column for source absolute growth as a function of direct primary energy.
        primary_energy[f"{source} (TWh growth - direct)"] = primary_energy.groupby("country")[
            f"{source} (TWh - direct)"
        ].diff()
        # Create column for source absolute growth as a function of input-equivalent primary energy.
        primary_energy[f"{source} (TWh growth - equivalent)"] = primary_energy.groupby("country")[
            f"{source} (TWh - equivalent)"
        ].diff()

    return primary_energy


def add_per_capita_variables(primary_energy: Table, ds_population: Dataset) -> Table:
    """Add per-capita variables.

    Parameters
    ----------
    primary_energy : Table
        Primary energy data.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    primary_energy : Table
        Data after adding per-capita variables.

    """
    primary_energy = primary_energy.copy()

    primary_energy = add_population_to_table(
        tb=primary_energy,
        ds_population=ds_population,
        warn_on_missing_countries=False,
    )
    for source in ONLY_DIRECT_ENERGY:
        primary_energy[f"{source} per capita (kWh)"] = (
            primary_energy[f"{source} (TWh)"] / primary_energy["population"] * TWH_TO_KWH
        )
    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        primary_energy[f"{source} per capita (kWh - direct)"] = (
            primary_energy[f"{source} (TWh - direct)"] / primary_energy["population"] * TWH_TO_KWH
        )
        primary_energy[f"{source} per capita (kWh - equivalent)"] = (
            primary_energy[f"{source} (TWh - equivalent)"] / primary_energy["population"] * TWH_TO_KWH
        )

    # Drop unnecessary column.
    primary_energy = primary_energy.drop(columns=["population"])

    return primary_energy


def prepare_output_table(primary_energy: Table) -> Table:
    """Create a table with the processed data, ready to be in a garden dataset and to be uploaded to grapher (although
    additional metadata may need to be added to the table).

    Parameters
    ----------
    primary_energy : Table
        Processed primary energy data.

    Returns
    -------
    table : catalog.Table
        Table, ready to be added to a new garden dataset.

    """
    primary_energy = primary_energy.copy()

    # Replace spurious inf values by nan.
    table = primary_energy.replace([np.inf, -np.inf], np.nan)

    # Sort conveniently and add an index.
    table = (
        table.sort_values(["country", "year"])
        .reset_index(drop=True)
        .set_index(["country", "year"], verify_integrity=True)
        # .astype({"Country code": "category"})
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

    # Set a new table short name.
    table.metadata.short_name = paths.short_name

    # Underescore table.
    table = table.underscore()

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the Statistical Review dataset and read its main table.
    ds_review: Dataset = paths.load_dependency("statistical_review_of_world_energy")
    tb_review = ds_review["statistical_review_of_world_energy"]

    # Load the population dataset.
    ds_population: Dataset = paths.load_dependency("population")

    #
    # Process data.
    #
    # Select necessary data from the Statistical Review.
    primary_energy = get_statistical_review_data(tb_review=tb_review)

    # Calculate direct and primary energy using the substitution method.
    primary_energy = calculate_direct_primary_energy(primary_energy=primary_energy)
    primary_energy = calculate_equivalent_primary_energy(primary_energy=primary_energy)

    # Calculate share of (direct and sub-method) primary energy.
    primary_energy = calculate_share_of_primary_energy(primary_energy=primary_energy)

    # Calculate annual change of primary energy.
    primary_energy = calculate_primary_energy_annual_change(primary_energy)

    # Add per-capita variables.
    primary_energy = add_per_capita_variables(primary_energy=primary_energy, ds_population=ds_population)

    # Prepare output data in a convenient way.
    table = prepare_output_table(primary_energy)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[table], default_metadata=ds_review.metadata, check_variables_metadata=True
    )
    ds_garden.save()
