"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder
from owid.datautils.dataframes import combine_two_overlapping_dataframes

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select and rename columns from Ember's yearly electricity.
COLUMNS_EMBER = {
    "country": "country",
    "year": "year",
    # Sources that can be perfectly mapped between Pinto's and Ember's data.
    "generation__total_generation__twh": "total_production",
    "generation__nuclear__twh": "nuclear_production",
    "generation__nuclear__pct": "nuclear_share",
    "generation__gas__twh": "gas_production",
    "generation__gas__pct": "gas_share",
    "generation__coal__twh": "coal_production",
    "generation__coal__pct": "coal_share",
    "generation__wind__twh": "wind_production",
    "generation__wind__pct": "wind_share",
    "generation__hydro__twh": "hydro_production",
    "generation__hydro__pct": "hydro_share",
    # Other categories that will be mapped (see EMBER_TO_PINTO_MAPPING below).
    "generation__solar__twh": "solar_production",
    "generation__solar__pct": "solar_share",
    "generation__other_fossil__twh": "other_fossil_production",
    "generation__other_fossil__pct": "other_fossil_share",
    "generation__other_renewables__twh": "other_renewables_production",
    "generation__other_renewables__pct": "other_renewables_share",
    "generation__bioenergy__twh": "bioenergy_production",
    "generation__bioenergy__pct": "bioenergy_share",
    # Additional groups that will be mapped (see ADDITIONAL_MAPPINGS below).
    "generation__fossil__twh": "fossil_production",
    "generation__fossil__pct": "fossil_share",
    "generation__hydro__bioenergy_and_other_renewables__twh": "hydro_bioenergy_and_other_renewables_production",
    "generation__hydro__bioenergy_and_other_renewables__pct": "hydro_bioenergy_and_other_renewables_share",
    "generation__gas_and_other_fossil__twh": "gas_and_other_fossil_production",
    "generation__gas_and_other_fossil__pct": "gas_and_other_fossil_share",
    "generation__other_fossil__twh": "other_fossil_production",
    "generation__other_fossil__pct": "other_fossil_share",
    "generation__wind_and_solar__twh": "wind_and_solar_production",
    "generation__wind_and_solar__pct": "wind_and_solar_share",
    "generation__renewables__twh": "renewables_production",
    "generation__renewables__pct": "renewables_share",
    "generation__clean__twh": "clean_production",
    "generation__clean__pct": "clean_share",
}


EMBER_TO_PINTO_MAPPING = {
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
ADDITIONAL_MAPPINGS = {
    # Derived groups:
    "fossil": ["coal", "gas", "other_fossil"],
    "hydro_bioenergy_and_other_renewables": ["hydro", "bioenergy", "other_renewables"],
    "gas_and_other_fossil": ["gas", "other_fossil"],
    "wind_and_solar": ["wind", "solar"],
    "renewables": ["hydro_bioenergy_and_other_renewables", "wind", "solar"],
    "clean": ["renewables", "nuclear"],
}


def sanity_check_data_overlap(tb, tb_latest, index_columns: list[str], *, plot=False) -> None:
    tb = tb.copy()
    tb_latest = tb_latest.copy()
    index_columns = ["country", "year"]
    common_columns = sorted((set(tb.columns) & set(tb_latest.columns)) - set(index_columns))
    for column in common_columns:
        tb_compared = tb[index_columns + [column]].merge(
            tb_latest[index_columns + [column]], on=index_columns, how="inner", suffixes=("_old", "_new")
        )
        tb_compared["pct_change"] = (
            100 * abs(tb_compared[f"{column}_old"] - tb_compared[f"{column}_new"]) / tb_compared[f"{column}_old"]
        )
        # Check that the differences are smaller than 10% (skipping rows where energy production is small).
        # NOTE: For bioenergy and other renewables, we allow a larger percentage difference. I couldn't figure out a better mapping for these groups.
        expected_pct_diff = 10
        if column in [
            "other_renewables_production",
            "other_renewables_share",
            "bioenergy_production",
            "bioenergy_share",
        ]:
            expected_pct_diff = 20
        error = f"Difference between Pinto and Ember data is larger than expected for: {column}"
        assert (tb_compared[tb_compared[f"{column}_old"] > 20]["pct_change"] < expected_pct_diff).all(), error

    if plot:
        import owid.catalog.processing as pr
        import plotly.express as px

        tb_compared = pr.concat(
            [tb.assign(**{"source": "Pinto et al."}), tb_latest.assign(**{"source": "Ember"})], ignore_index=True
        )
        for column in [column for column in tb_compared.columns if column not in index_columns if column != "source"]:
            if len(set(tb_compared.dropna(subset=column)["source"])) == 1:
                continue
            px.line(tb_compared, x="year", y=column, color="source", markers=True).show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset on global historical electricity.
    ds_historical = paths.load_dataset("global_historical_electricity")
    # Read table on the total electricity production (and consumption).
    tb_total = ds_historical.read("electricity_production_and_consumption")
    # Read table on the share of electricity production by source.
    tb_share = ds_historical.read("electricity_production_share_by_source")

    # Load garden dataset of Ember's yearly electricity data.
    ds_latest = paths.load_dataset("yearly_electricity")
    # Read its main table.
    tb_latest = ds_latest.read("yearly_electricity")

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
        # Map Pinto's sources onto Ember's sources (see definitions and comments above in EMBER_TO_PINTO_MAPPING).
        for mapping_ember, mapping_pinto in EMBER_TO_PINTO_MAPPING.items():
            columns_pinto = [f"{column}_{column_suffix}" for column in mapping_pinto]
            tb[f"{mapping_ember}_{column_suffix}"] = tb[columns_pinto].sum(axis=1)
            tb = tb.drop(columns=columns_pinto, errors="raise")

        # Create additional groups (as defined in Ember's data step) by combining Pinto's sources.
        for mapping_ember, mapping_pinto in ADDITIONAL_MAPPINGS.items():
            columns_pinto = [f"{column}_{column_suffix}" for column in mapping_pinto]
            tb[f"{mapping_ember}_{column_suffix}"] = tb[columns_pinto].sum(axis=1)

    # Select and rename columns from Ember conveniently.
    tb_latest = tb_latest[list(COLUMNS_EMBER)].rename(columns=COLUMNS_EMBER, errors="raise")
    # Select only global data from Ember.
    tb_latest = tb_latest[tb_latest["country"] == "World"].reset_index(drop=True)

    # Compare Pinto's and Ember's data for different sources.
    # Turn plot=True for visual inspection.
    sanity_check_data_overlap(tb, tb_latest, index_columns=["country", "year"], plot=False)

    # Combine global historical data from Pinto with Ember's global data.
    # Where there's overlap, prioritize Ember's recent data.
    tb = combine_two_overlapping_dataframes(df1=tb_latest, df2=tb, index_columns=["country", "year"])

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_historical.metadata)

    # Save garden dataset.
    ds_garden.save()
