"""Deforestation embedded in trade, from the DeDuCE physical trade model."""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

INDEX_COLUMNS = [
    "producer_country",
    "consumer_country",
    "commodity_group",
    "year",
]
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


def sanity_check_outputs(tb: Table, totals_input: Table) -> None:
    assert set(tb["commodity_group"]) == COMMODITY_GROUPS, "Unexpected commodity groups."
    assert not tb.duplicated(subset=INDEX_COLUMNS).any(), "Duplicated rows after aggregation."
    assert (tb["deforestation_risk"] >= 0).all(), "Negative deforestation area."
    totals_output = tb.groupby("year", observed=True)[VALUE_COLUMNS].sum()
    assert np.allclose(
        totals_output.to_numpy(dtype=float), totals_input.loc[totals_output.index].to_numpy(dtype=float), rtol=1e-9
    ), "Yearly world totals changed after aggregation."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("deforestation_embedded_in_trade")
    tb = ds_meadow.read("deforestation_embedded_in_trade")

    #
    # Process data.
    #
    # Convert emissions from million tonnes to tonnes of CO2.
    tb["deforestation_emissions"] *= 1e6

    totals_input = tb.groupby("year", observed=True)[VALUE_COLUMNS].sum()

    for column in ["producer_country", "consumer_country"]:
        tb = paths.regions.harmonize_names(
            tb, country_col=column, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
        )

    tb = tb.groupby(INDEX_COLUMNS, observed=True)[VALUE_COLUMNS].sum().reset_index()

    sanity_check_outputs(tb, totals_input)

    tb = tb.format(INDEX_COLUMNS)

    #
    # Save outputs.
    #
    # Keep float64 precision: repacking would store the values as float32.
    ds_garden = paths.create_dataset(tables=[tb], repack=False)
    ds_garden.save()
