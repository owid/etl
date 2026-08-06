"""Garden step that combines EI's statistical review with Ember's yearly electricity data to create the Electricity Mix
(EI & Ember) dataset.

"""

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from shared import EXCLUDED_PROVIDER_REGIONS
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12


def process_statistical_review_data(tb_review: Table) -> Table:
    """Load necessary columns from EI's Statistical Review dataset, and create some new variables (e.g. electricity
    generation from fossil fuels).

    Parameters
    ----------
    table_ei : Table
        EI's Statistical Review (already processed, with harmonized countries and region aggregates).

    Returns
    -------
    tb_review : Table
        Processed EI data.

    """
    # Columns to load from EI dataset.
    columns = {
        "electricity_generation_twh": "total_generation__twh",
        "hydro_electricity_generation_twh": "hydro_generation__twh",
        "nuclear_electricity_generation_twh": "nuclear_generation__twh",
        "solar_electricity_generation_twh": "solar_generation__twh",
        "wind_electricity_generation_twh": "wind_generation__twh",
        "other_renewables_electricity_generation_twh": "other_renewables_including_bioenergy_generation__twh",
        "oil_electricity_generation_twh": "oil_generation__twh",
        "coal_electricity_generation_twh": "coal_generation__twh",
        "gas_electricity_generation_twh": "gas_generation__twh",
        # Total energy supply, the denominator of the share of electricity in primary energy. It is dropped once
        # that share is calculated.
        "total_energy_supply_twh": "total_energy_supply__twh",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns, errors="raise")
    # New columns to be created by summing other columns.
    aggregates: dict[str, list[str]] = {
        "fossil_generation__twh": [
            "oil_generation__twh",
            "coal_generation__twh",
            "gas_generation__twh",
        ],
        "renewable_generation__twh": [
            "hydro_generation__twh",
            "solar_generation__twh",
            "wind_generation__twh",
            "other_renewables_including_bioenergy_generation__twh",
        ],
        "low_carbon_generation__twh": [
            "renewable_generation__twh",
            "nuclear_generation__twh",
        ],
        "solar_and_wind_generation__twh": [
            "solar_generation__twh",
            "wind_generation__twh",
        ],
    }

    # Create a table with a dummy index.
    tb_review = tb_review.reset_index()

    # Create new columns, by adding up other columns (and allowing for zero nans in each sum).
    for new_column in aggregates:
        tb_review[new_column] = tb_review[aggregates[new_column]].sum(axis=1, min_count=len(aggregates[new_column]))

    return tb_review


def process_ember_data(tb_ember: Table) -> Table:
    """Load necessary columns from the Yearly Electricity dataset and prepare a table with the required variables.

    Parameters
    ----------
    table_ember : Table
        Yearly Electricity Data.

    Returns
    -------
    df_ember : Table
        Processed Yearly Electricity data.

    """
    # Columns to load from Ember dataset.
    columns = {
        "generation__bioenergy__twh": "bioenergy_generation__twh",
        "generation__gas__twh": "gas_generation__twh",
        "generation__coal__twh": "coal_generation__twh",
        "generation__other_fossil__twh": "oil_generation__twh",
        "generation__renewables__twh": "renewable_generation__twh",
        "generation__other_renewables__twh": "other_renewables_excluding_bioenergy_generation__twh",
        "generation__clean__twh": "low_carbon_generation__twh",
        "generation__hydro__twh": "hydro_generation__twh",
        "generation__nuclear__twh": "nuclear_generation__twh",
        "generation__solar__twh": "solar_generation__twh",
        "generation__wind__twh": "wind_generation__twh",
        "generation__fossil__twh": "fossil_generation__twh",
        "generation__total_generation__twh": "total_generation__twh",
        "demand__total_demand__twh": "total_demand__twh",
        "emissions__lifecycle__total_emissions__mtco2": "total_emissions__mtco2",
        "emissions__lifecycle__co2_intensity__gco2_kwh": "co2_intensity__gco2_kwh",
        "imports__total_net_imports__twh": "total_net_imports__twh",
    }
    tb_ember = tb_ember[list(columns)].rename(columns=columns, errors="raise")

    # Create a table with a dummy index.
    tb_ember = tb_ember.reset_index()

    # In EI data, there is a variable "Geo Biomass Other", which combines all other renewables.
    # In Ember data, "other renewables" excludes bioenergy.
    # To be able to combine both datasets, create a new variable for generation of other renewables including bioenergy.
    # Instead of simply summing them, we allow one of them to be missing, otherwise, we miss significant data; for example, UK has "Other renewables" data until 2019, and none afterwards.
    tb_ember["other_renewables_including_bioenergy_generation__twh"] = tb_ember[
        ["other_renewables_excluding_bioenergy_generation__twh", "bioenergy_generation__twh"]
    ].sum(axis=1, min_count=1)

    # Create a new variable for solar and wind generation.
    tb_ember["solar_and_wind_generation__twh"] = tb_ember["solar_generation__twh"] + tb_ember["wind_generation__twh"]

    return tb_ember


def sanity_check_inputs(tb_review, tb_ember):
    # There are only 3 countries/regions (besides special EI regions) that are in the EI and not in Ember. They are Curacao, Netherlands Antilles, and USSR.
    # And the data of the former two is all zero.
    error = "Unexpected list of countries in EI that are not in Ember."
    assert set([c for c in tb_review["country"] if "(EI)" not in c]) - set(
        [c for c in tb_ember["country"] if "(Ember)" not in c]
    ) == {"Curacao", "Netherlands Antilles", "USSR"}, error
    error = "All data for Curacao and Netherlands Antilles was expected to be zero."
    assert (
        (
            tb_review[tb_review["country"].isin(["Curacao", "Netherlands Antilles"])]
            .fillna(0)
            .drop(columns=["country", "year"])
            == 0
        )
        .all()
        .all()
    ), error
    # So the only country that we get from the EI is USSR; all other countries are in Ember.

    # Total energy supply is the one column that comes from EI alone (Ember reports electricity only).
    error = "Unexpected columns found in EI and not found in Ember."
    assert set(tb_review.columns) - set(tb_ember.columns) == {"total_energy_supply__twh"}, error

    # There are also some columns that are only in Ember and not in EI.
    error = "Unexpected columns found in Ember and not found in EI."
    assert set(tb_ember.columns) - set(tb_review.columns) == {
        "bioenergy_generation__twh",
        "co2_intensity__gco2_kwh",
        "other_renewables_excluding_bioenergy_generation__twh",
        "total_demand__twh",
        "total_emissions__mtco2",
        "total_net_imports__twh",
    }, error

    # The majority of columns are informed in both EI and Ember.
    # However, Ember's data starts in 1999 (for EU countries) and in 2000 (for the rest), whereas EI data starts earlier (in 1965 or 1985).
    error = "EI data coverage has changed unexpectedly."
    ei_coverage = {column: int(tb_review.dropna(subset=column)["year"].min()) for column in tb_review.columns}
    assert set(ei_coverage.values()) == {1965, 1985}, error
    error = "Ember data coverage has changed unexpectedly."
    ember_coverage = {column: int(tb_ember.dropna(subset=column)["year"].min()) for column in tb_ember.columns}
    assert set(ember_coverage.values()) == {1990, 2000}, error


def combine_ei_and_ember_data(tb_review, tb_ember):
    # Drop Curacao and Netherland Antilles, which have only zeros in the data.
    tb_review = tb_review[~tb_review["country"].isin(["Curacao", "Netherlands Antilles"])].reset_index(drop=True)

    # Initialize a combined table, which is a copy of Ember's table.
    combined = tb_ember.copy()

    # Add unique EI columns to Ember (outer merge).
    # Above, in sanity_check_inputs, we asserted that they are expected to be biofuel, coal, gas and oil consumption, as well as primary energy consumption.
    ei_unique_columns = sorted(set(tb_review.columns) - set(tb_ember.columns))
    combined = combined.merge(tb_review[["country", "year"] + ei_unique_columns], how="outer", on=["country", "year"])

    # Combine EI and Ember data, selecting only pre-2000 EI data and only non-EI specific regions. Prioritize Ember on overlapping values.
    # This will automatically add the USSR data (which is fully contained in pre-2000 EI table).
    ei_countries = [country for country in set(tb_review["country"]) if "(EI)" in country]
    combined = combine_two_overlapping_dataframes(
        df1=combined,
        df2=tb_review[(tb_review["year"] < 2000) & ~(tb_review["country"].isin(ei_countries))],
        index_columns=["country", "year"],
    )

    # Combine them again, for all years and only EI regions.
    combined = combine_two_overlapping_dataframes(
        df1=combined, df2=tb_review[(tb_review["country"].isin(ei_countries))], index_columns=["country", "year"]
    )

    # In the original Statistical Review data, countries that have no nuclear power had no data for nuclear generation.
    # Ideally, missing data should mean "unknown", and therefore those missing values should be zero instead of nan.
    # In the Statistical Review garden step, we filled out those nans with zeros, for country-years that certainly have no nuclear generation.
    # The same issue occurs with Ember data: countries with no nuclear power have missing data for nuclear generation (e.g. Australia), instead of zeros.
    # We could copy the function we used in the Statistical Review garden step here.
    # But, to avoid repeating code, we will take all zeros in nuclear generation from the EI data as a new temporary column; then, we'll fill nans in the combined table with those zeros.
    # NOTE: We don't need to do this for pre-2000 data, since we already combined Ember with pre-2000 EI data.
    combined = combined.merge(
        tb_review[(tb_review["nuclear_generation__twh"] == 0) & (tb_review["year"] >= 2000)][
            ["country", "year", "nuclear_generation__twh"]
        ],
        on=["country", "year"],
        how="outer",
        suffixes=("", "_temp"),
    )
    combined["nuclear_generation__twh"] = combined["nuclear_generation__twh"].fillna(
        combined["nuclear_generation__twh_temp"]
    )
    combined = combined.drop(columns=["nuclear_generation__twh_temp"])

    return combined


def derive_other_renewables_excluding_bioenergy(combined: Table) -> Table:
    """Fill 'other renewables excluding bioenergy' where it is missing but derivable.

    Ember reports the combined 'other renewables including bioenergy' and, separately, bioenergy, but
    not always the geothermal/wave/tidal remainder ('excluding'). Wherever the combined figure and
    bioenergy are both present but the remainder is missing, derive it as excluding = including -
    bioenergy (the three are defined so that including = excluding + bioenergy, so this is exact).
    Country-years that have only the combined figure (no bioenergy) are left missing on purpose.
    """
    inc = "other_renewables_including_bioenergy_generation__twh"
    exc = "other_renewables_excluding_bioenergy_generation__twh"
    bio = "bioenergy_generation__twh"
    residual = combined[inc] - combined[bio]
    n_negative = int((residual < -1e-6).sum())
    if n_negative:
        # Tiny mismatches between EI's combined figure and Ember's bioenergy; clip to zero.
        log.warning("electricity_mix.other_renewables_excluding.negative_residual_clipped", n=n_negative)
    derived = residual.clip(lower=0)
    n_filled = int((combined[exc].isna() & derived.notna()).sum())
    log.info("electricity_mix.derive_other_renewables_excluding", n_filled=n_filled)
    combined[exc] = combined[exc].fillna(derived)
    return combined


def add_bioenergy_split_helper_columns(combined: Table) -> Table:
    """Add gap-filled helper columns so the stacked 'by source' views can show bioenergy separately
    while keeping full historical coverage.

    A stacked chart only renders years where every series has a value, so a sparse bioenergy series would
    otherwise collapse each country's history to the years the split exists. These two helpers always sum
    to the true 'other renewables including bioenergy' total, so the stack keeps full coverage:
      - other_renewables_generation__twh: other renewables excluding bioenergy where known (post-2000 and
        anywhere the split is available), falling back to the combined figure for the earlier years where
        bioenergy cannot be separated. There, its biomass is carried in this 'other renewables' band.
      - bioenergy_stacked_generation__twh: bioenergy with missing values set to zero, so it stacks across
        the full period (zero in the early years whose biomass is instead carried by other renewables).
    The clean, unfilled bioenergy_generation__twh and other_renewables_excluding/including columns are kept
    for the standalone (non-stacked) source views.
    """
    inc = "other_renewables_including_bioenergy_generation__twh"
    exc = "other_renewables_excluding_bioenergy_generation__twh"
    combined["other_renewables_generation__twh"] = combined[exc].fillna(combined[inc])
    combined["bioenergy_stacked_generation__twh"] = combined["bioenergy_generation__twh"].fillna(0)
    return combined


# Generation variables (TWh) that become per-capita (kWh/person) and share-of-electricity (%) columns.
# Defined at module level so the monthly table builder reuses the exact same lists as the annual data.
PER_CAPITA_VARIABLES = [
    "bioenergy_generation__twh",
    "coal_generation__twh",
    "fossil_generation__twh",
    "gas_generation__twh",
    "hydro_generation__twh",
    "low_carbon_generation__twh",
    "nuclear_generation__twh",
    "oil_generation__twh",
    "other_renewables_excluding_bioenergy_generation__twh",
    "other_renewables_including_bioenergy_generation__twh",
    "other_renewables_generation__twh",
    "bioenergy_stacked_generation__twh",
    "renewable_generation__twh",
    "solar_generation__twh",
    "total_generation__twh",
    "total_demand__twh",
    "wind_generation__twh",
    "solar_and_wind_generation__twh",
]
SHARE_VARIABLES = [
    "bioenergy_generation__twh",
    "coal_generation__twh",
    "fossil_generation__twh",
    "gas_generation__twh",
    "hydro_generation__twh",
    "low_carbon_generation__twh",
    "nuclear_generation__twh",
    "oil_generation__twh",
    "other_renewables_excluding_bioenergy_generation__twh",
    "other_renewables_including_bioenergy_generation__twh",
    "renewable_generation__twh",
    "solar_generation__twh",
    "total_generation__twh",
    "wind_generation__twh",
    "solar_and_wind_generation__twh",
]


def add_per_capita_variables(combined: Table) -> Table:
    """Add per capita variables (in kWh per person) to the combined EI and Ember table.

    The list of variables to make per capita are given in this function. The new variable names will be 'per_capita_'
    followed by the original variable's name.

    Parameters
    ----------
    combined : Table
        Combination of EI's Statistical Review and Ember's Yearly Electricity Data.
    ds_population: Dataset
        Population dataset.

    Returns
    -------
    combined : Table
        Input table after adding per capita variables.

    """
    combined = combined.copy()

    # Add a column for population (only for harmonized countries).
    combined = paths.regions.add_population(tb=combined, warn_on_missing_countries=False)

    # Variables to make per capita (shared with the monthly builder).
    for variable in PER_CAPITA_VARIABLES:
        assert "twh" in variable, f"Variables are assumed to be in TWh, but {variable} is not."
        new_column = "per_capita_" + variable.replace("__twh", "__kwh")
        combined[new_column] = combined[variable] * TWH_TO_KWH / combined["population"]

    return combined


def add_share_variables(combined: Table) -> Table:
    """Share-of-electricity (%), share of electricity in primary energy, and net-imports-share variables."""
    # Each source's generation as a share of total generation (shared with the monthly builder).
    for variable in SHARE_VARIABLES:
        new_column = variable.replace("_generation__twh", "_share_of_electricity__pct")
        combined[new_column] = 100 * combined[variable] / combined["total_generation__twh"]

    # Share of primary energy that is generated as electricity, where primary energy is total energy
    # supply (the same quantity we report as primary energy everywhere else).
    combined["total_electricity_share_of_primary_energy__pct"] = (
        100 * combined["total_generation__twh"] / combined["total_energy_supply__twh"]
    )
    # Drop unnecessary columns.
    combined = combined.drop(columns=["total_energy_supply__twh"], errors="raise")

    # Calculate the percentage of electricity demand that is imported.
    combined["net_imports_share_of_demand__pct"] = (
        100 * combined["total_net_imports__twh"] / combined["total_demand__twh"]
    )

    # Sanity check.
    error = "Total electricity share does not add up to 100%."
    assert all(abs(combined["total_share_of_electricity__pct"].dropna() - 100) < 0.01), error

    # Remove unnecessary columns.
    combined = combined.drop(columns=["total_share_of_electricity__pct"], errors="raise")

    return combined


def fix_discrepancies_in_aggregate_regions(tb_review: Table, tb_ember: Table, combined: Table) -> Table:
    # Firstly, remove "Other * (EI)" regions. They come from the Statistical Review, to include data that is not accounted for in any country. They needed to be included to be able to create region aggregates. But Ember doesn't have these regions. If we keep them, they lead to inconsistencies, e.g. electricity shares larger than 100%.
    combined = combined[~combined["country"].str.contains(r"Other.*\(EI\)", regex=True)].reset_index(drop=True)

    # Define the maximum median relative error between Statistical Review and Ember (for a given region and indicator).
    # If the error is larger than this, we will only take Ember data.
    maximum_median_error = 0.2
    # Define the regions and indicators where the median error is exceeded.
    segments_not_combined = {region: [] for region in geo.REGIONS}
    segments_not_combined.update(
        {
            "Low-income countries": [],
            "Lower-middle-income countries": [
                "fossil_generation__twh",
                "gas_generation__twh",
                "hydro_generation__twh",
                "low_carbon_generation__twh",
                "oil_generation__twh",
                "other_renewables_including_bioenergy_generation__twh",
                "renewable_generation__twh",
            ],
            "Upper-middle-income countries": ["oil_generation__twh"],
            "High-income countries": ["oil_generation__twh"],
            "Europe": ["oil_generation__twh"],
            "North America": [],
            "European Union (27)": ["oil_generation__twh"],
            "Africa": [],
            "Asia": [],
            "Oceania": [],
            "South America": [],
        }
    )
    drifted = {}
    for region in segments_not_combined:
        _remove_combination = []
        for col in combined.drop(columns=["country", "year"]).columns:
            if (col in tb_review.columns) and (col in tb_ember.columns):
                compared = pd.merge(
                    tb_review[tb_review["country"] == region][["year", col]].dropna(),
                    tb_ember[tb_ember["country"] == region][["year", col]].dropna(),
                    how="inner",
                    on="year",
                    suffixes=("_review", "_ember"),
                )
                if len(compared) > 0:
                    median_error = np.median(
                        (abs(compared[f"{col}_review"] - compared[f"{col}_ember"])) / abs(compared[f"{col}_ember"])
                    )
                    if median_error > maximum_median_error:
                        _remove_combination.append(col)
                        # DEBUGGING: Uncomment to plot.
                        # px.line(compared.melt(id_vars="year"), x="year", y="value", color="variable", markers=True, title=f"{region} - {col}").show()
                        assert compared["year"].min() == 1990 if region == "European Union (27)" else 2000, (
                            "Minimum year changed."
                        )
        if set(segments_not_combined[region]) != set(_remove_combination):
            drifted[region] = sorted(_remove_combination)

        for col in _remove_combination:
            # Remove data for years prior to 2000 (which correspond to the Statistical Review).
            # NOTE: This may need to be generalized if Ember adds data prior to 2000 (which is the case already for European countries, but they are so far not affected by the discrepancies).
            combined.loc[(combined["country"] == region) & (combined["year"] < 2000), col] = np.nan

    assert not drifted, (
        "Expected discrepancies between Statistical Review and Ember data for aggregate regions have changed. "
        f"Update 'segments_not_combined' with the measured lists: {drifted}"
    )

    return combined


def check_carbon_intensity(combined: Table) -> None:
    # There is already a carbon intensity variable in the Ember dataset, but now that we have combined EI and Ember data, intensities might need to be recalculated for consistency.
    # However, before doing that, check if it's necessary. If the resulting calculated intensity is very similar to the original, just keep the original.
    _combined = combined.copy()
    _combined["_co2_intensity"] = (combined["total_emissions__mtco2"] * MT_TO_G) / (
        combined["total_generation__twh"] * TWH_TO_KWH
    )
    error = "Carbon intensities differ from expected values by more than the tolerance. Consider recalculating intensities or increasing the relative tolerance."
    assert (
        _combined[
            ~np.isclose(
                _combined["_co2_intensity"].to_numpy(),
                _combined["co2_intensity__gco2_kwh"].to_numpy(),
                rtol=1e-3,
                equal_nan=True,
            )
        ][["country", "year", "_co2_intensity", "co2_intensity__gco2_kwh"]]
        .dropna()
        .empty
    ), error
    # If the assertion is not fulfilled, consider simply recalculating intensities.
    # combined["co2_intensity__gco2_kwh"] = co2_intensity.copy()


# Mapping from the columns of the global historical electricity dataset (Pinto et al. + Ember, World only)
# to the electricity mix columns. Only generation columns are grafted; per capita and share variables are
# recomputed afterwards. Shares from the historical dataset are ignored (recomputed from generation).
# NOTE: "other_fossil" in the historical dataset (oil + waste + peat) is used as a proxy for oil-fired
# generation; before ~1965, non-coal, non-gas fossil generation is dominated by oil, so this is a good
# approximation for the long-run tail.
HISTORICAL_ELECTRICITY_COLUMNS = {
    "total_production": "total_generation__twh",
    "coal_production": "coal_generation__twh",
    "gas_production": "gas_generation__twh",
    "other_fossil_production": "oil_generation__twh",
    "nuclear_production": "nuclear_generation__twh",
    "hydro_production": "hydro_generation__twh",
    "solar_production": "solar_generation__twh",
    "wind_production": "wind_generation__twh",
    "bioenergy_production": "bioenergy_generation__twh",
    "other_renewables_production": "other_renewables_excluding_bioenergy_generation__twh",
    "fossil_production": "fossil_generation__twh",
    "renewables_production": "renewable_generation__twh",
    "clean_production": "low_carbon_generation__twh",
    "wind_and_solar_production": "solar_and_wind_generation__twh",
}


def check_historical_overlap(tb_historical: Table, combined: Table) -> None:
    """Check that Pinto's historical World series agrees with the modern (Ember/SR) World series.

    The two sources overlap from 2000 onwards. Since Pinto's granular sources are mapped onto the
    standard categories (and, e.g., its "other fossil" is used as a proxy for oil), we allow a
    generous tolerance; the check is only meant to catch a broken mapping. On the overlap, modern
    data is kept anyway (Pinto only fills the earlier years).
    """
    index_columns = ["country", "year"]
    # The "other renewables including bioenergy" aggregate is not checked: Pinto groups combustible
    # renewables, geothermal and tidal/wave differently from Ember, so the reconstructed aggregate is
    # inherently fuzzy. Its base components (bioenergy, other renewables) are checked individually.
    columns_to_skip = {"other_renewables_including_bioenergy_generation__twh"}
    common_columns = sorted((set(tb_historical.columns) & set(combined.columns)) - set(index_columns) - columns_to_skip)
    world_historical = tb_historical[tb_historical["country"] == "World"]
    world_modern = combined[combined["country"] == "World"]
    violations = []
    for column in common_columns:
        compared = world_historical[index_columns + [column]].merge(
            world_modern[index_columns + [column]], on=index_columns, suffixes=("_historical", "_modern")
        )
        # Skip small values (in TWh), where relative differences are noisy and irrelevant.
        compared = compared[compared[f"{column}_modern"] > 20].dropna()
        if compared.empty:
            continue
        pct_change = (
            100
            * (compared[f"{column}_historical"] - compared[f"{column}_modern"]).abs()
            / compared[f"{column}_modern"].abs()
        )
        # Allow a larger tolerance for categories whose mapping is known to be approximate.
        tolerance = 25 if any(s in column for s in ["oil", "other_renewables", "bioenergy"]) else 15
        if (pct_change >= tolerance).any():
            violations.append(f"{column} (max {pct_change.max():.1f}%, tolerance {tolerance}%)")
    assert not violations, (
        "Pinto's historical World electricity disagrees with modern data beyond tolerance for: "
        + "; ".join(violations)
        + ". The mapping from Pinto's sources may need revisiting."
    )


def add_historical_electricity(combined: Table, tb_historical: Table) -> Table:
    """Extend the World electricity series back to ~1900 using Pinto et al.'s global historical data.

    Where the modern electricity mix (Ember and the Statistical Review) already has data, it is kept;
    the historical dataset only fills the earlier years (roughly 1900-1964).
    """
    tb_historical = tb_historical[["country", "year"] + list(HISTORICAL_ELECTRICITY_COLUMNS)].rename(
        columns=HISTORICAL_ELECTRICITY_COLUMNS, errors="raise"
    )

    # Reconstruct "other renewables including bioenergy" from its two components.
    tb_historical["other_renewables_including_bioenergy_generation__twh"] = tb_historical[
        ["bioenergy_generation__twh", "other_renewables_excluding_bioenergy_generation__twh"]
    ].sum(axis=1, min_count=1)

    # Sanity check that Pinto's historical data agrees with modern data over their overlap.
    check_historical_overlap(tb_historical=tb_historical, combined=combined)

    # Combine, prioritizing the modern electricity mix over the historical data on overlapping years.
    combined = combine_two_overlapping_dataframes(df1=combined, df2=tb_historical, index_columns=["country", "year"])

    return combined


# Mapping from the UK BEIS historical electricity columns to the electricity mix columns.
UK_BEIS_COLUMNS = {
    "coal": "coal_generation__twh",
    "oil": "oil_generation__twh",
    "gas": "gas_generation__twh",
    "hydro": "hydro_generation__twh",
    "nuclear": "nuclear_generation__twh",
    "electricity_generation": "total_generation__twh",
    "net_imports": "total_net_imports__twh",
}


def add_uk_historical_electricity(combined: Table, tb_beis: Table) -> Table:
    """Extend the United Kingdom's electricity series back to ~1920 with BEIS historical data.

    BEIS has the lowest priority: it only fills UK years before the modern data (Ember and the Statistical
    Review) begins, around 1985. BEIS reports fuel *input* for fossil fuels, so their generation is
    estimated with BEIS's implied efficiency. Wind, solar and other renewables were negligible in that
    period, so they are set to zero to complete the historical mix.

    Only the generation columns get BEIS as an origin (last, since it is the lowest-priority source);
    demand, emissions and carbon intensity are not in BEIS, so those series stay on the modern period.
    """
    tb_beis = tb_beis.reset_index()[
        ["country", "year"] + list(UK_BEIS_COLUMNS) + ["implied_efficiency", "wind_and_solar"]
    ].rename(columns=UK_BEIS_COLUMNS, errors="raise")
    tb_beis = tb_beis[tb_beis["country"] == "United Kingdom"].reset_index(drop=True)

    # BEIS reports fuel input for fossil fuels; convert to electricity generation via its implied efficiency.
    for column in ["coal_generation__twh", "oil_generation__twh", "gas_generation__twh"]:
        tb_beis[column] *= tb_beis["implied_efficiency"]

    # First year the modern UK data reports solar or wind generation.
    modern_first_year = combined[
        (combined["country"] == "United Kingdom")
        & (combined["solar_generation__twh"].notna() | combined["wind_generation__twh"].notna())
    ]["year"].min()
    # BEIS gives wind and solar combined; confirm it is negligible before the modern data begins.
    assert tb_beis[tb_beis["year"] < modern_first_year]["wind_and_solar"].fillna(0).max() == 0, (
        "BEIS wind+solar is no longer negligible before the modern data begins; revisit this assumption."
    )
    # Set solar, wind and other renewables to zero for the historical period, and leave the modern years
    # to the higher-priority sources.
    zero_columns = [
        "solar_generation__twh",
        "wind_generation__twh",
        "other_renewables_including_bioenergy_generation__twh",
    ]
    for column in zero_columns:
        tb_beis[column] = 0.0
    tb_beis.loc[tb_beis["year"] >= modern_first_year, zero_columns] = np.nan
    tb_beis = tb_beis.drop(columns=["implied_efficiency", "wind_and_solar"], errors="raise")

    # Recompute the aggregate generation columns from the base sources for the historical period.
    tb_beis["fossil_generation__twh"] = tb_beis[
        ["coal_generation__twh", "oil_generation__twh", "gas_generation__twh"]
    ].sum(axis=1, min_count=3)
    tb_beis["renewable_generation__twh"] = tb_beis[
        [
            "hydro_generation__twh",
            "solar_generation__twh",
            "wind_generation__twh",
            "other_renewables_including_bioenergy_generation__twh",
        ]
    ].sum(axis=1, min_count=4)
    tb_beis["low_carbon_generation__twh"] = tb_beis[["renewable_generation__twh", "nuclear_generation__twh"]].sum(
        axis=1, min_count=2
    )
    tb_beis["solar_and_wind_generation__twh"] = tb_beis[["solar_generation__twh", "wind_generation__twh"]].sum(
        axis=1, min_count=2
    )

    # Combine, prioritizing the modern data; BEIS only fills earlier UK years.
    combined = combine_two_overlapping_dataframes(df1=combined, df2=tb_beis, index_columns=["country", "year"])

    return combined


def add_per_capita_variables_monthly(tb: Table) -> Table:
    """Per-capita (kWh per person) variables for the monthly table.

    Population is annual, so each month uses its calendar-year population.
    """
    tb = tb.copy()
    tb["year"] = tb["date"].dt.year
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    for variable in PER_CAPITA_VARIABLES:
        new_column = "per_capita_" + variable.replace("__twh", "__kwh")
        tb[new_column] = tb[variable] * TWH_TO_KWH / tb["population"]
    tb = tb.drop(columns=["year", "population"], errors="raise")
    return tb


def add_share_variables_monthly(tb: Table) -> Table:
    """Share-of-electricity (%) and net-imports-share variables for the monthly table.

    Skips the share of electricity in primary energy, which is an annual, Statistical-Review-only metric.
    """
    for variable in SHARE_VARIABLES:
        new_column = variable.replace("_generation__twh", "_share_of_electricity__pct")
        tb[new_column] = 100 * tb[variable] / tb["total_generation__twh"]
    tb["net_imports_share_of_demand__pct"] = 100 * tb["total_net_imports__twh"] / tb["total_demand__twh"]
    error = "Total electricity share does not add up to 100%."
    assert all(abs(tb["total_share_of_electricity__pct"].dropna() - 100) < 0.01), error
    tb = tb.drop(columns=["total_share_of_electricity__pct"], errors="raise")
    return tb


def build_monthly_electricity_mix() -> Table:
    """Build the Ember-only monthly electricity mix table, parallel to the annual combined table.

    Reuses the same Ember processing as the annual data (rename, aggregates, the bioenergy /
    other-renewables split and gap-filled helpers), but without combining other sources (the monthly
    data is Ember-only) and indexed by date instead of year.
    """
    ds_ember_monthly = paths.load_dataset("monthly_electricity")
    tb = ds_ember_monthly.read("monthly_electricity", reset_index=False)
    tb = process_ember_data(tb_ember=tb)
    tb = derive_other_renewables_excluding_bioenergy(combined=tb)
    tb = add_bioenergy_split_helper_columns(combined=tb)
    tb = add_per_capita_variables_monthly(tb=tb)
    tb = add_share_variables_monthly(tb=tb)
    tb = tb.format(keys=["country", "date"], sort_columns=True, short_name="electricity_mix_monthly")
    return tb


def run() -> None:
    #
    # Load data.
    #
    # Load EI's statistical review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    # Load Ember's yearly electricity dataset and read its main table.
    ds_ember = paths.load_dataset("yearly_electricity")
    tb_ember = ds_ember.read("yearly_electricity", reset_index=False)

    # Load the global historical electricity dataset (Pinto et al. + Ember), used to extend the World series back to ~1900.
    ds_historical = paths.load_dataset("global_historical_electricity")
    tb_historical = ds_historical.read("global_historical_electricity")

    # Load the UK BEIS historical electricity dataset, used to extend the UK series back to ~1920.
    ds_beis = paths.load_dataset("uk_historical_electricity")
    tb_beis = ds_beis.read("uk_historical_electricity")

    # Load population dataset.

    #
    # Process data.
    #
    # Prepare EI and Ember data.
    tb_review = process_statistical_review_data(tb_review=tb_review)
    tb_ember = process_ember_data(tb_ember=tb_ember)

    # Sanity check inputs.
    sanity_check_inputs(tb_review=tb_review, tb_ember=tb_ember)

    ####################################################################################################################
    # There is a big discrepancy between Oceania's oil generation from the Energy Institute and Ember.
    # Ember's oil generation is significantly larger. The reason seems to be that the Energy Institute's Statistical
    # Review has spurious zeros for Papua New Guinea and New Caledonia (all electricity columns are zero)
    # while Ember does have data for both countries.
    # Therefore, to avoid spurious jumps in the intersection between EI and Ember data, we remove Oceania data from EI
    # before combining both tables.
    # Specifically, the columns where the discrepancy between EI and Ember is notorious are oil and gas generation (and
    # therefore fossil generation).

    # First check that indeed there is no data for Papua New Guinea and New Caledonia in EI.
    error = "Expected all electricity data for Papua New Guinea and New Caledonia to be zero in the Statistical Review."
    assert (
        (
            tb_review[tb_review["country"].isin(["Papua New Guinea", "New Caledonia"])].fillna(0)[
                [c for c in tb_review.columns if c not in ["country", "year"]]
            ]
            == 0
        )
        .all()
        .all()
    ), error
    affected_columns = ["oil_generation__twh", "gas_generation__twh", "fossil_generation__twh"]
    tb_review.loc[tb_review["country"] == "Oceania", affected_columns] = None

    # We also remove all electricity data for these countries from the Statistical Review, given that they are all zero
    # (most of them spurious).
    tb_review.loc[
        (tb_review["country"].isin(["Papua New Guinea", "New Caledonia"])),
        tb_review.drop(columns=["country", "year"]).columns,
    ] = None

    # Coal generation in Ember data is missing.
    # The reason may be that Switzerland stopped using coal for electricity before year 2000:
    # https://data.worldbank.org/indicator/EG.ELC.COAL.ZS?locations=CH
    # Ideally, the data should be zero, instead of missing.
    error = "Expected missing data for Switzerland coal generation. That may no longer be the case. Remove this code."
    assert (
        tb_ember.loc[(tb_ember["country"] == "Switzerland") & (tb_ember["year"] > 1999)]["coal_generation__twh"]
        .isnull()
        .all()
    ), error
    tb_ember.loc[(tb_ember["country"] == "Switzerland") & (tb_ember["year"] > 1999), "coal_generation__twh"] = 0
    ####################################################################################################################

    # Combine EI and Ember data.
    combined = combine_ei_and_ember_data(tb_review=tb_review, tb_ember=tb_ember)

    # Extend the World electricity series back to ~1900 with Pinto et al.'s global historical data.
    combined = add_historical_electricity(combined=combined, tb_historical=tb_historical)

    # Remove combined data for aggregate regions where Ember and the Statistical Review have a strong disagreement.
    # This way we avoid spurious jumps in the combined series.
    combined = fix_discrepancies_in_aggregate_regions(tb_review=tb_review, tb_ember=tb_ember, combined=combined)

    # Extend the United Kingdom series back to ~1920 with BEIS historical data (lowest priority).
    combined = add_uk_historical_electricity(combined=combined, tb_beis=tb_beis)

    # Check if carbon intensity needs to be recalculated.
    check_carbon_intensity(combined=combined)

    # Derive 'other renewables excluding bioenergy' where it is missing but recoverable from the
    # combined figure and bioenergy (case B). Done before per-capita and share so they inherit it.
    combined = derive_other_renewables_excluding_bioenergy(combined=combined)

    # Gap-filled helper columns so the stacked 'by source' views can separate bioenergy with full history.
    combined = add_bioenergy_split_helper_columns(combined=combined)

    # Add per capita variables.
    combined = add_per_capita_variables(combined=combined)

    # Add "share" variables.
    combined = add_share_variables(combined=combined)

    # Format table conveniently.
    # Remove residual and undefined provider regions (kept in the Statistical Review garden as
    # aggregation inputs, but meaningless to readers).
    combined = combined[~combined["country"].isin(EXCLUDED_PROVIDER_REGIONS)].reset_index(drop=True)

    combined = combined.format(sort_columns=True, short_name=paths.short_name)

    # Build the Ember-only monthly table (a parallel frequency; the annual data above is untouched).
    tb_monthly = build_monthly_electricity_mix()

    #
    # Save outputs.
    #
    # Create a new garden dataset with both the annual and the monthly tables.
    ds_garden = paths.create_dataset(tables=[combined, tb_monthly])
    ds_garden.save()
