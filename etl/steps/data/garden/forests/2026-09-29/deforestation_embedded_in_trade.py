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
    # The source lists some flows under several commodity names that refer to the same FAOSTAT item, with identical
    # values. The trade model works with FAOSTAT trade data, which has no detail below the item, so these rows are
    # copies of the same flow, and summing them would count it two or three times. We keep one row per flow and item,
    # using the producers' lookup table to get each commodity's FAOSTAT item code.
    duplicates = [
        {"Mangoes", "Guavas", "Mangoes, guavas and mangosteens"},
        {"Pulses, n.e.c.", "Other pulses n.e.c."},
        {"Stimulant, spice and aromatic crops, n.e.c.", "Other stimulant, spice and aromatic crops, n.e.c."},
        {"Tangerines and mandarins", "Tangerines, mandarins, clementines"},
        {"Other tropical fruits, n.e.c.", "Other tropical and subtropical fruits, n.e.c."},
    ]
    tb["fao_item_code"] = tb["commodity"].map(tb_lookup.set_index("commodity")["fao_item_code"])
    assert tb["fao_item_code"].notna().all(), "Commodities missing from the lookup."
    names_by_item = tb.groupby("fao_item_code")["commodity"].agg(set)
    error = "The commodities sharing a FAOSTAT item changed. This issue may have been fixed; review this block."
    assert sorted(sorted(s) for s in names_by_item if len(s) > 1) == sorted(sorted(s) for s in duplicates), error
    flow_columns = ["producer_country", "consumer_country", "year", "fao_item_code"]
    copies = tb.groupby(flow_columns, observed=True)[VALUE_COLUMNS + ["consumption"]].nunique(dropna=False)
    assert (copies == 1).all().all(), "Copies of a flow are not identical; sum them instead of dropping them."
    n_rows = len(tb)
    tb = tb.drop_duplicates(subset=flow_columns).drop(columns="fao_item_code")
    # Each extra name added one full copy of its flows, so exactly those copies should be gone.
    n_copies = sum((len(names) - 1) * tb["commodity"].isin(names).sum() for names in duplicates)
    assert n_rows - len(tb) == n_copies, "Unexpected number of repeated rows."
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
