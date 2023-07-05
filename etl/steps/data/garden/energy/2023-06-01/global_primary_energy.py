"""Garden step that combines Vaclav Smil's Global Primary Energy with BP's Statistical Review of World Energy.

"""

import numpy as np
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset_with_combined_metadata

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Exajoules to terawatt-hours.
EJ_TO_TWH = 1e6 / 3600

# Average efficiency factor assumed to convert direct energy to input-equivalent energy of Smil's data.
# This factor will be used for hydropower, nuclear, other renewables, solar and wind
# (for which there is data until 1960).
# In practice, it only affects hydropower, since all other non-fossil sources are zero prior to 1960.
# All other energy sources in Smil's data will not be affected by this factor.
EFFICIENCY_FACTOR = 0.36


def prepare_bp_data(tb_bp: Table) -> Table:
    tb_bp = tb_bp.reset_index()

    # BP gives generation of direct energy in TWh, and, for non-fossil sources of electricity,
    # consumption of input-equivalent energy in EJ.
    # The input-equivalent energy is the amount of energy that would be required to generate a given amount of (direct)
    # electricity if non-fossil sources were as inefficient as a standard thermal power plant.
    # Therefore, direct and substituted energies for Biofuels, Coal, Gas and Oil are identical.
    # On the other hand, direct and substituted energy are different for non-fossil electricity sources, namely
    # Hydropower, Nuclear, Solar, Other renewables, and Wind.
    # The difference is of a factor of ~38%, which is roughly the efficiency of a standard power plant.
    # More specifically, BP assumes (for Biofuels, Coal, Gas and Oil) an efficiency factor that grows from 36%
    # (until year 2000) to 40.6% (in 2021), to better reflect changes in efficiency over time.
    # In the case of biomass used in electricity (included in 'Other renewables'),
    # BP assumes a constant factor of 32% for all years.
    # For more details:
    # https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2022-methodology.pdf
    bp_columns = {
        "country": "country",
        "year": "year",
        # Fossil sources (direct energy).
        "biofuels_consumption__twh__total": "biofuels__twh_direct_energy",
        "coal_consumption__twh": "coal__twh_direct_energy",
        "gas_consumption__twh": "gas__twh_direct_energy",
        "oil_consumption__twh": "oil__twh_direct_energy",
        # Non-fossil electricity sources (direct energy).
        "geo_biomass_other__twh": "other_renewables__twh_direct_energy",
        "hydro_generation__twh": "hydropower__twh_direct_energy",
        "nuclear_generation__twh": "nuclear__twh_direct_energy",
        "solar_generation__twh": "solar__twh_direct_energy",
        "wind_generation__twh": "wind__twh_direct_energy",
        # Non-fossil electricity sources (substituted energy).
        "geo_biomass_other__ej": "other_renewables__ej_substituted_energy",
        "hydro_consumption__ej": "hydropower__ej_substituted_energy",
        "nuclear_consumption__ej": "nuclear__ej_substituted_energy",
        "solar_consumption__ej": "solar__ej_substituted_energy",
        "wind_consumption__ej": "wind__ej_substituted_energy",
    }
    tb_bp = tb_bp[list(bp_columns)].rename(columns=bp_columns)
    # Convert all units to TWh.
    for column in tb_bp.columns:
        if "_ej_" in column:
            # Create a new column in TWh instead of EJ.
            tb_bp[column.replace("_ej_", "_twh_")] = tb_bp[column] * EJ_TO_TWH
            # Remove the column in EJ.
            tb_bp = tb_bp.drop(columns=column)
    # For completeness, create columns of substituted energy for fossil sources (even if they would coincide with
    # direct energy).
    for fossil_source in ["biofuels", "coal", "gas", "oil"]:
        tb_bp[f"{fossil_source}__twh_substituted_energy"] = tb_bp[f"{fossil_source}__twh_direct_energy"]

    # Select only data for the World (which is the only region informed in Smil's data).
    tb_bp = tb_bp[tb_bp["country"] == "World"].reset_index(drop=True)

    return tb_bp


def prepare_smil_data(tb_smil: Table) -> Table:
    tb_smil = tb_smil.reset_index()

    # Create columns for input-equivalent energy.
    # To do this, we follow a similar approach to BP:
    # We create input-equivalent energy by dividing direct energy consumption of non-fossil electricity sources
    # (hydropower, nuclear, other renewables, solar and wind) by a factor of 36%
    # (called EFFICIENCY_FACTOR, defined above).
    # This is the efficiency factor of a typical thermal plant assumed by BP between 1965 and 2000, and we assume this
    # factor also applies for the period 1800 to 1965.
    # For biomass power (included in other renewables), BP assumed a constant factor of 32%.
    # However, since we cannot separate biomass from the rest of sources in 'other renewables',
    # we use the same 36% factor as all other non-fossil sources.
    for source in ["hydropower", "nuclear", "other_renewables", "solar", "wind"]:
        tb_smil[f"{source}__twh_substituted_energy"] = tb_smil[f"{source}__twh_direct_energy"] / EFFICIENCY_FACTOR
    # For fossil sources (including biofuels and traditional biomass), direct and substituted energy are the same.
    for source in ["biofuels", "coal", "gas", "oil", "traditional_biomass"]:
        tb_smil[f"{source}__twh_substituted_energy"] = tb_smil[f"{source}__twh_direct_energy"]

    return tb_smil


def combine_bp_and_smil_data(tb_bp: Table, tb_smil: Table) -> Table:
    tb_bp = tb_bp.copy()
    tb_smil = tb_smil.copy()

    # Add a new column that informs of the source of the data.
    tb_bp["data_source"] = "BP"
    tb_smil["data_source"] = "Smil"
    # Combine both tables, prioritizing BP's data on overlapping rows.
    combined = combine_two_overlapping_dataframes(
        df1=tb_bp, df2=tb_smil, index_columns=["country", "year"]
    ).sort_values(["year"])

    # Replace <NA> by numpy nans.
    combined = combined.fillna(np.nan)

    # We do not have data for traditional biomass after 2015 (BP does not provide it).
    # So, to be able to visualize the complete mix of global energy consumption,
    # we extrapolate Smil's data for traditional biomass from 2015 onwards, by repeating its last value.
    missing_years_mask = combined["year"] >= tb_smil["year"].max()
    combined.loc[missing_years_mask, "traditional_biomass__twh_direct_energy"] = combined[missing_years_mask][
        "traditional_biomass__twh_direct_energy"
    ].ffill()
    combined.loc[missing_years_mask, "traditional_biomass__twh_substituted_energy"] = combined[missing_years_mask][
        "traditional_biomass__twh_substituted_energy"
    ].ffill()
    for source in ["hydropower", "nuclear", "other_renewables", "solar", "wind"]:
        combined[
            f"{source}__twh_substituted_energy"
        ].metadata.description = 'Figures are based on gross generation and do not account for cross-border electricity supply. "Input-equivalent" energy is the amount of fuel that would be required by thermal power stations to generate the reported electricity output.'

    # Create an index and sort conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return combined


def add_total_consumption_and_percentages(combined: Table) -> Table:
    # Create a column with the total direct energy (ensuring there is at least one non-nan value).
    combined["total_consumption__twh_direct_energy"] = combined[
        [column for column in combined.columns if "direct_energy" in column]
    ].sum(axis=1, min_count=1)
    # Create a column with the total substituted energy (ensuring there is at least one non-nan value).
    combined["total_consumption__twh_substituted_energy"] = combined[
        [column for column in combined.columns if "substituted_energy" in column]
    ].sum(axis=1, min_count=1)
    # Add share variables.
    sources = [
        "biofuels",
        "coal",
        "gas",
        "hydropower",
        "nuclear",
        "oil",
        "other_renewables",
        "solar",
        "traditional_biomass",
        "wind",
    ]
    for source in sources:
        # Add percentage of each source with respect to the total direct energy.
        combined[f"{source}__pct_of_direct_energy"] = (
            100 * combined[f"{source}__twh_direct_energy"] / combined["total_consumption__twh_direct_energy"]
        )
        # Add percentage of each source with respect to the total substituted energy.
        combined[f"{source}__pct_of_substituted_energy"] = (
            100 * combined[f"{source}__twh_substituted_energy"] / combined["total_consumption__twh_substituted_energy"]
        )

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BP statistical review dataset and read its main table.
    ds_bp: Dataset = paths.load_dependency("statistical_review")
    tb_bp = ds_bp["statistical_review"]

    # Load Smil dataset and read its main table.
    ds_smil: Dataset = paths.load_dependency("global_primary_energy")
    tb_smil = ds_smil["global_primary_energy"]

    #
    # Process data.
    #
    # Prepare BP data.
    tb_bp = prepare_bp_data(tb_bp=tb_bp)

    # Prepare Smil data.
    tb_smil = prepare_smil_data(tb_smil=tb_smil)

    # Combine BP and Smil data.
    combined = combine_bp_and_smil_data(tb_bp=tb_bp, tb_smil=tb_smil)

    # Add variables for total consumption and variables of % share of each source.
    combined = add_total_consumption_and_percentages(combined=combined)

    # Update table name.
    combined.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Save garden dataset.
    ds_garden = create_dataset_with_combined_metadata(dest_dir, datasets=[ds_bp, ds_smil], tables=[combined])
    ds_garden.save()
