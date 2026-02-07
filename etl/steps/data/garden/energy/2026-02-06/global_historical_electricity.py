"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select and rename columns from Ember's yearly electricity.
COLUMNS_EMBER = {
    "country": "country",
    "year": "year",
    "generation__nuclear__twh": "nuclear_production",
    "generation__solar__twh": "solar_production",
    "generation__total_generation__twh": "total_production",
    "generation__wind__pct": "wind_share",
    "generation__nuclear__pct": "nuclear_share",
    # We should probably not merge Pinto's oil with Ember's "other fossil", given that the latter includes:
    # "Other Fossil generation includes generation from oil and petroleum products, as well as manufactured gases and waste."
    # After inspection, indeed, Ember's "other_fossil" is somewhat larger than Pinto's "oil", although just by a few percent.
    # For now, skip these columns.
    # 'generation__other_fossil__pct': 'oil_share',
    # 'generation__other_fossil__twh': 'oil_production',
    "generation__solar__pct": "solar_share",
    "generation__gas__pct": "gas_share",
    "generation__coal__twh": "coal_production",
    "generation__hydro__pct": "hydro_share",
    "generation__coal__pct": "coal_share",
    "generation__gas__twh": "gas_production",
    "generation__wind__twh": "wind_production",
    "generation__hydro__twh": "hydro_production",
    # TODO: Check how to map other renwables from both datasets. For now, ignore them.
    # 'generation__bioenergy__pct': 'bioenergy__pct',
    # 'generation__other_renewables__twh': 'other_renewables__twh',
    # 'generation__hydro__bioenergy_and_other_renewables__twh': 'hydro__bioenergy_and_other_renewables__twh',
    # 'generation__other_renewables__pct': 'other_renewables__pct',
    # 'generation__bioenergy__twh': 'bioenergy__twh',
    # 'generation__hydro__bioenergy_and_other_renewables__pct': 'hydro__bioenergy_and_other_renewables__pct',
}


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

    # Combine solar PV and thermal.
    # NOTE: Ember's "solar" includes both. As stated in their methodology:
    # "Solar includes both solar thermal and solar photovoltaic generation, and where possible distributed solar generation is included."
    for column_suffix in ["production", "share"]:
        tb[f"solar_{column_suffix}"] = tb[f"solar_photovoltaic_{column_suffix}"] + tb[f"solar_thermal_{column_suffix}"]
        tb = tb.drop(columns=[f"solar_photovoltaic_{column_suffix}", f"solar_thermal_{column_suffix}"], errors="raise")

    #
    # "Other Fossil generation includes generation from oil and petroleum products, as well as manufactured gases and waste."

    # Select and rename columns from Ember conveniently.
    tb_latest = tb_latest[list(COLUMNS_EMBER)].rename(columns=COLUMNS_EMBER, errors="raise")
    # Select only global data from Ember.
    tb_latest = tb_latest[tb_latest["country"] == "World"].reset_index(drop=True)

    ####################################################################################################################
    # TODO: Add assertion to check that, where they overlap, they agree within a few percent. Then pack into a function.
    # Compare production shares of both datasets and assert that they agree reasonably well.
    # import owid.catalog.processing as pr
    # tb_compared = pr.concat([tb.assign(**{"source": "Pinto et al."}), tb_latest.assign(**{"source": "Ember"})], ignore_index=True)
    # import plotly.express as px
    # # for column in [column for column in tb_latest.columns if column.endswith("_share")]:
    # for column in [column for column in tb_latest.columns if column not in ["country", "year"]]:
    #     px.line(tb_compared, x="year", y=column, color="source", markers=True).show()
    ####################################################################################################################

    # Combine global historical data from Pinto et al. (2023) with Ember's global data.
    from owid.datautils.dataframes import combine_two_overlapping_dataframes

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
