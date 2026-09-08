"""Garden step for Pinto et al.'s global historical electricity data (World only).

This step processes the digitized historical electricity data from Pinto et al. into a World-level
long-run series of electricity generation by source, mapped onto the standard source categories.

It does NOT merge with Ember or the Statistical Review; that combination (and the sanity check that
the historical data agrees with modern data over their overlap) happens in the electricity_mix step.
This keeps this step a clean, single-source processing of Pinto's data.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping of Pinto's granular sources onto the standard (Ember) source categories.
PINTO_TO_STANDARD_MAPPING = {
    # According to Ember's methodology:
    # "Solar includes both solar thermal and solar photovoltaic generation, and where possible distributed solar generation is included."
    "solar": ["solar_photovoltaic", "solar_thermal"],
    # According to Ember's methodology:
    # "Other Renewables generation includes geothermal, tidal and wave generation."
    # NOTE: Pinto's geothermal + tidal, wave and ocean is somewhat larger than Ember's other renewables.
    "other_renewables": ["geothermal", "tidal_wave_ocean"],
    # According to Ember's methodology:
    # "Other Fossil generation includes generation from oil and petroleum products, as well as manufactured gases and waste.
    "other_fossil": ["oil", "waste", "peat"],
    # Map Pinto's Combustible renewables to Ember's Bioenergy.
    # NOTE: Visually, this mapping is not perfect, they differ within 20%.
    "bioenergy": ["combustible_renewables"],
}
# Derived groups (combining the standard categories above), following Ember's definitions.
ADDITIONAL_MAPPINGS = {
    "fossil": ["coal", "gas", "other_fossil"],
    "hydro_bioenergy_and_other_renewables": ["hydro", "bioenergy", "other_renewables"],
    "gas_and_other_fossil": ["gas", "other_fossil"],
    "wind_and_solar": ["wind", "solar"],
    "renewables": ["hydro_bioenergy_and_other_renewables", "wind", "solar"],
    "clean": ["renewables", "nuclear"],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset on global historical electricity (Pinto et al.).
    ds_historical = paths.load_dataset("global_historical_electricity")
    # Read table on the total electricity production (and consumption).
    tb_total = ds_historical.read("electricity_production_and_consumption")
    # Read table on the share of electricity production by source.
    tb_share = ds_historical.read("electricity_production_share_by_source")

    #
    # Process data.
    #
    # Rename sources conveniently in share table.
    tb_share = tb_share.rename(columns={"natural_gas": "gas", "tide_wave__ocean": "tidal_wave_ocean"}, errors="raise")

    # Create a combined table with total electricity production, and the share of each source.
    tb = tb_total.drop(columns=["electricity_consumption"]).merge(tb_share, on=["year"], how="outer")

    for column in [column for column in tb.columns if column not in ["year", "electricity_production"]]:
        # Add a column with the electricity produced by each source.
        tb[f"{column}_production"] = tb["electricity_production"] * tb[column]
        # Rename share column conveniently.
        tb = tb.rename(columns={column: f"{column}_share"}, errors="raise")
        # Make share columns percentages.
        tb[f"{column}_share"] *= 100

    # Rename columns conveniently.
    tb = tb.rename(columns={"electricity_production": "total_production"}, errors="raise")

    # Add a country column.
    tb["country"] = "World"

    for column_suffix in ["production", "share"]:
        # Map Pinto's granular sources onto the standard source categories (see PINTO_TO_STANDARD_MAPPING).
        for standard_source, pinto_sources in PINTO_TO_STANDARD_MAPPING.items():
            columns_pinto = [f"{column}_{column_suffix}" for column in pinto_sources]
            tb[f"{standard_source}_{column_suffix}"] = tb[columns_pinto].sum(axis=1)
            tb = tb.drop(columns=columns_pinto, errors="raise")

        # Create additional groups (as defined in Ember's data step) by combining standard categories.
        for standard_source, component_sources in ADDITIONAL_MAPPINGS.items():
            columns_pinto = [f"{column}_{column_suffix}" for column in component_sources]
            tb[f"{standard_source}_{column_suffix}"] = tb[columns_pinto].sum(axis=1)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_historical.metadata)

    # Save garden dataset.
    ds_garden.save()
