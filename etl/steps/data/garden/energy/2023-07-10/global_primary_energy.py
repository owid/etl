"""Garden step that combines Vaclav Smil's data on global primary energy with the Energy Institute Statistical Review of
World Energy.

"""

import numpy as np
from owid.catalog import Dataset, Table
from owid.catalog.tables import (
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
)
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Average efficiency factor assumed to convert direct energy to input-equivalent energy of Smil's data.
# This factor will be used for hydropower, nuclear, other renewables, solar and wind
# (for which there is data until 1960).
# In practice, it only affects hydropower, since all other non-fossil sources are zero prior to 1960.
# All other energy sources in Smil's data will not be affected by this factor.
EFFICIENCY_FACTOR = 0.36


def prepare_statistical_review_data(tb_review: Table) -> Table:
    tb_review = tb_review.reset_index()

    # The Statistical Review gives generation of energy in TWh, and, for non-fossil sources of electricity,
    # consumption of input-equivalent energy in EJ.
    # The input-equivalent energy is the amount of energy that would be required to generate a given amount of (direct)
    # electricity if non-fossil sources were as inefficient as a standard thermal power plant.
    # Therefore, direct and substituted energies for Biofuels, Coal, Gas and Oil are identical.
    # On the other hand, direct and substituted energy are different for non-fossil electricity sources, namely
    # Hydropower, Nuclear, Solar, Other renewables, and Wind.
    # The difference is of a factor of ~38%, which is roughly the efficiency of a standard power plant.
    # More specifically, the Statistical Review assumes (for Biofuels, Coal, Gas and Oil) an efficiency factor that
    # grows from 36% (until year 2000) to 40.6% (in 2021), to better reflect changes in efficiency over time.
    # In the case of biomass used in electricity (included in 'Other renewables'),
    # the Statistical Review assumes a constant factor of 32% for all years.
    # For more details:
    # https://www.energyinst.org/statistical-review/about
    # https://www.energyinst.org/__data/assets/pdf_file/0003/1055541/Methodology.pdf
    # NOTES:
    # * We already converted input-equivalent energy from EJ to TWh in the garden step of the Statistical Review.
    # * We included estimates of direct energy (by multiplying by the efficiency factors of the table in the appendix
    #   of the Statistical Review), however, we will use the electricity generation instead, as an estimate for direct
    #   primary energy. The estimated direct energy and electricity generation are almost identical, except for other
    #   renewables, where there the estimated direct primary energy is about 24% larger than electricity generation.
    columns = {
        "country": "country",
        "year": "year",
        # Fossil sources (direct energy).
        "biofuels_consumption_twh": "biofuels__twh_direct_energy",
        "coal_consumption_twh": "coal__twh_direct_energy",
        "gas_consumption_twh": "gas__twh_direct_energy",
        "oil_consumption_twh": "oil__twh_direct_energy",
        # Non-fossil electricity sources (direct energy).
        "other_renewables_electricity_generation_twh": "other_renewables__twh_direct_energy",
        "hydro_electricity_generation_twh": "hydropower__twh_direct_energy",
        "nuclear_electricity_generation_twh": "nuclear__twh_direct_energy",
        "solar_electricity_generation_twh": "solar__twh_direct_energy",
        "wind_electricity_generation_twh": "wind__twh_direct_energy",
        # Non-fossil electricity sources (substituted energy).
        "other_renewables_consumption_equivalent_twh": "other_renewables__twh_substituted_energy",
        "hydro_consumption_equivalent_twh": "hydropower__twh_substituted_energy",
        "nuclear_consumption_equivalent_twh": "nuclear__twh_substituted_energy",
        "solar_consumption_equivalent_twh": "solar__twh_substituted_energy",
        "wind_consumption_equivalent_twh": "wind__twh_substituted_energy",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns, errors="raise")
    # For completeness, create columns of substituted energy for fossil sources (even if they would coincide with
    # direct energy).
    for fossil_source in ["biofuels", "coal", "gas", "oil"]:
        tb_review[f"{fossil_source}__twh_substituted_energy"] = tb_review[f"{fossil_source}__twh_direct_energy"]

    # Select only data for the World (which is the only region informed in Smil's data).
    tb_review = tb_review[tb_review["country"] == "World"].reset_index(drop=True)

    return tb_review


def prepare_smil_data(tb_smil: Table) -> Table:
    tb_smil = tb_smil.reset_index()

    # Create columns for input-equivalent energy.
    # To do this, we follow a similar approach to the Statistical Review:
    # We create input-equivalent energy by dividing direct energy consumption of non-fossil electricity sources
    # (hydropower, nuclear, other renewables, solar and wind) by a factor of 36%
    # (called EFFICIENCY_FACTOR, defined above).
    # This is the efficiency factor of a typical thermal plant assumed by the Statistical Review between 1965 and 2000,
    # and we assume this factor also applies for the period 1800 to 1965.
    # For biomass power (included in other renewables), the Statistical Review assumed a constant factor of 32%.
    # However, since we cannot separate biomass from the rest of sources in 'other renewables',
    # we use the same 36% factor as all other non-fossil sources.
    for source in ["hydropower", "nuclear", "other_renewables", "solar", "wind"]:
        tb_smil[f"{source}__twh_substituted_energy"] = tb_smil[f"{source}__twh_direct_energy"] / EFFICIENCY_FACTOR
    # For fossil sources (including biofuels and traditional biomass), direct and substituted energy are the same.
    for source in ["biofuels", "coal", "gas", "oil", "traditional_biomass"]:
        tb_smil[f"{source}__twh_substituted_energy"] = tb_smil[f"{source}__twh_direct_energy"]

    return tb_smil


def combine_statistical_review_and_smil_data(tb_review: Table, tb_smil: Table) -> Table:
    tb_review = tb_review.copy()
    tb_smil = tb_smil.copy()

    # Add a new column that informs of the source of the data (either the Energy Institute or Smil (2017)).
    tb_review["data_source"] = "EI"
    tb_smil["data_source"] = "Smil"
    # Combine both tables, prioritizing the Statistical Review's data on overlapping rows.
    # NOTE: Currently, function combine_two_overlapping_dataframes does not properly propagate metadata.
    #  For now, sources and licenses have to be combined manually.
    index_columns = ["country", "year"]
    combined = combine_two_overlapping_dataframes(df1=tb_review, df2=tb_smil, index_columns=index_columns).sort_values(
        ["year"]
    )
    # Combine metadata of the two tables.
    sources = get_unique_sources_from_tables([tb_review, tb_smil])
    licenses = get_unique_licenses_from_tables([tb_review, tb_smil])
    for column in combined.drop(columns=index_columns).columns:
        combined[column].metadata.sources = sources
        combined[column].metadata.licenses = licenses

    # Update the name of the new combined table.
    combined.metadata.short_name = paths.short_name

    # Replace <NA> by numpy nans.
    combined = combined.fillna(np.nan)

    # We do not have data for traditional biomass after 2015 (the Statistical Review does not provide it).
    # So, to be able to visualize the complete mix of global energy consumption,
    # we extrapolate Smil's data for traditional biomass from 2015 onwards, by repeating its last value.
    missing_years_mask = combined["year"] >= tb_smil["year"].max()
    combined.loc[missing_years_mask, "traditional_biomass__twh_direct_energy"] = combined[missing_years_mask][
        "traditional_biomass__twh_direct_energy"
    ].ffill()
    combined.loc[missing_years_mask, "traditional_biomass__twh_substituted_energy"] = combined[missing_years_mask][
        "traditional_biomass__twh_substituted_energy"
    ].ffill()

    # Create an index and sort conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return combined


def add_total_consumption_and_percentages(combined: Table) -> Table:
    # Create a column with the total direct energy (ensuring there is at least one non-nan value).
    direct_energy_columns = [column for column in combined.columns if "direct_energy" in column]
    combined["total_consumption__twh_direct_energy"] = combined[direct_energy_columns].sum(axis=1, min_count=1)
    # Create a column with the total substituted energy (ensuring there is at least one non-nan value).
    equivalent_energy_columns = [column for column in combined.columns if "substituted_energy" in column]
    combined["total_consumption__twh_substituted_energy"] = combined[equivalent_energy_columns].sum(axis=1, min_count=1)
    # The previous operations do not propagate metadata; do it manually.
    combined["total_consumption__twh_direct_energy"].metadata.sources = get_unique_sources_from_tables(
        [combined[direct_energy_columns]]
    )
    combined["total_consumption__twh_direct_energy"].metadata.licenses = get_unique_licenses_from_tables(
        [combined[direct_energy_columns]]
    )
    combined["total_consumption__twh_substituted_energy"].metadata.sources = get_unique_sources_from_tables(
        [combined[equivalent_energy_columns]]
    )
    combined["total_consumption__twh_substituted_energy"].metadata.licenses = get_unique_licenses_from_tables(
        [combined[equivalent_energy_columns]]
    )

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
    # Load Statistical Review dataset and read its main table.
    ds_review: Dataset = paths.load_dependency("statistical_review_of_world_energy")
    tb_review = ds_review["statistical_review_of_world_energy"]

    # Load Smil dataset and read its main table.
    ds_smil: Dataset = paths.load_dependency("smil_2017")
    tb_smil = ds_smil["smil_2017"]

    #
    # Process data.
    #
    # Prepare Statistical Review data.
    tb_review = prepare_statistical_review_data(tb_review=tb_review)

    # Prepare Smil data.
    tb_smil = prepare_smil_data(tb_smil=tb_smil)

    # Combine Statistical Review and Smil data.
    combined = combine_statistical_review_and_smil_data(tb_review=tb_review, tb_smil=tb_smil)

    # Add variables for total consumption and variables of % share of each source.
    combined = add_total_consumption_and_percentages(combined=combined)

    #
    # Save outputs.
    #
    # Save garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[combined], default_metadata=ds_review.metadata, check_variables_metadata=True
    )
    ds_garden.save()
