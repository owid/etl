"""Deforestation embedded in trade, from the DeDuCE physical trade model."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

INDEX_COLUMNS = ["producer_country", "consumer_country", "commodity_group", "year"]
VALUE_COLUMNS = ["deforestation_risk", "deforestation_emissions"]

COMMODITY_GROUPS = {
    "Cereals",
    "Edible roots and tubers with high starch or inulin content",
    "Fibre crops",
    "Fruit and nuts",
    "Oilseeds and oleaginous fruits",
    "Pasture",
    "Pulses (dried leguminous vegetables)",
    "Stimulant, spice and aromatic crops",
    "Vegetables",
}


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("deforestation_embedded_in_trade")
    tb = ds_meadow.read("deforestation_embedded_in_trade")

    #
    # Process data.
    #
    assert set(tb["commodity_group"]) == COMMODITY_GROUPS, "Unexpected commodity groups."
    assert (tb["deforestation_risk"] >= 0).all(), "Negative deforestation area."

    # Convert emissions from million tonnes to tonnes of CO2.
    tb["deforestation_emissions"] *= 1e6
    assert 1e8 < tb["deforestation_emissions"].max() < 1e10, "Emissions are not in tonnes of CO2."

    for column in ["producer_country", "consumer_country"]:
        tb = paths.regions.harmonize_names(
            tb, country_col=column, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
        )

    # Sum the source's commodities into its commodity groups.
    tb = tb.groupby(INDEX_COLUMNS, observed=True)[VALUE_COLUMNS].sum().reset_index()

    tb = tb.format(INDEX_COLUMNS)

    #
    # Save outputs.
    #
    # Keep float64 precision: repacking would store the values as float32.
    ds_garden = paths.create_dataset(tables=[tb], repack=False)
    ds_garden.save()
