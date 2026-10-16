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
    tb_lookup = ds_meadow.read("deforestation_commodity_lookup")

    #
    # Process data.
    #
    assert set(tb["commodity_group"]) == COMMODITY_GROUPS, "Unexpected commodity groups."
    assert (tb["deforestation_risk"] >= 0).all(), "Negative deforestation area."

    # Convert emissions from million tonnes to tonnes of CO2.
    tb["deforestation_emissions"] *= 1e6
    assert 1e8 < tb["deforestation_emissions"].max() < 1e10, "Emissions are not in tonnes of CO2."

    ####################################################################################################################
    # The source lists some flows under several commodity names that share one FAOSTAT item, with identical values on
    # every row. For example, "Mangoes", "Guavas" and "Mangoes, guavas and mangosteens" are all FAOSTAT item 571. The
    # trade model works with FAOSTAT trade data, which has no detail below the item, so these rows are copies of the
    # same flow, and summing them would count it two or three times. We keep one row per flow and FAOSTAT item.
    # The producers' lookup table gives each commodity its FAOSTAT item code.
    expected_duplicates = {
        "Mangoes, guavas and mangosteens": ["Mangoes", "Guavas"],
        "Other pulses n.e.c.": ["Pulses, n.e.c."],
        "Other stimulant, spice and aromatic crops, n.e.c.": ["Stimulant, spice and aromatic crops, n.e.c."],
        "Tangerines, mandarins, clementines": ["Tangerines and mandarins"],
        "Other tropical fruits, n.e.c.": ["Other tropical and subtropical fruits, n.e.c."],
    }
    item_code = tb_lookup.set_index("commodity")["fao_item_code"].to_dict()
    tb["fao_item_code"] = tb["commodity"].map(item_code)
    error = f"Commodities missing from the lookup: {sorted(set(tb[tb['fao_item_code'].isna()]['commodity']))}"
    assert tb["fao_item_code"].notna().all(), error
    # Names that share a FAOSTAT item, among the commodities in the data.
    commodities = tb[["commodity", "fao_item_code"]].drop_duplicates()
    found = {frozenset(group["commodity"]) for _, group in commodities.groupby("fao_item_code") if len(group) > 1}
    expected = {frozenset([kept] + extras) for kept, extras in expected_duplicates.items()}
    error = "The commodities sharing a FAOSTAT item changed. This issue may have been fixed; review this block."
    assert found == expected, error
    # All copies of a flow must be identical, otherwise they should be summed, not dropped.
    flow_columns = ["producer_country", "consumer_country", "year", "fao_item_code"]
    copies = tb.groupby(flow_columns, observed=True)[VALUE_COLUMNS + ["consumption"]].nunique()
    assert (copies <= 1).all().all(), "Rows sharing a FAOSTAT item are no longer identical; sum them instead."
    tb = tb.drop_duplicates(subset=flow_columns).drop(columns="fao_item_code")
    ####################################################################################################################

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
