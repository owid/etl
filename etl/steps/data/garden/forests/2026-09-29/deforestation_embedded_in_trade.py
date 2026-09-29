"""Deforestation embedded in trade, from the DeDuCE physical trade model.

Produces two tables:

  * `deforestation_embedded_in_trade`: bilateral flows of amortized deforestation risk (hectares)
    and associated emissions (tonnes of CO2), summed from the source's 161 commodities
    into its 9 commodity groups, indexed by producer country, consumer country, commodity group
    and year.
  * `countries`: the source's ISO code and region ("country group") of each country. The source
    defines its own 8 regions; the bespoke deforestation sankey shows them next to each country.
"""

import numpy as np
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

VALUE_COLUMNS = ["deforestation_risk", "deforestation_emissions"]

EXPECTED_COMMODITY_GROUPS = {
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
EXPECTED_REGIONS = {
    "Africa",
    "Europe",
    "North Asia",
    "North and Central America",
    "Oceania",
    "Rest of Asia",
    "South America",
    "Southeast Asia",
}
# Trade model estimates start in 2005 (deforestation is amortized over the five preceding years).
EXPECTED_YEARS = set(range(2005, 2024))


def sanity_check_inputs(tb: Table) -> None:
    assert tb[VALUE_COLUMNS].notna().all().all(), "Missing values in the source file."
    assert (tb["deforestation_risk"] >= 0).all(), "Negative deforestation area in the source file."
    assert set(tb["commodity_group"]) == EXPECTED_COMMODITY_GROUPS, "Unexpected commodity groups in the source file."
    assert set(tb["producer_region"]) | set(tb["consumer_region"]) == EXPECTED_REGIONS, (
        "Unexpected regions in the source file."
    )
    assert set(tb["year"]) == EXPECTED_YEARS, f"Unexpected years in the source file: {sorted(set(tb['year']))}"
    assert not tb.duplicated(subset=["producer_country", "consumer_country", "commodity", "year"]).any(), (
        "Duplicated rows in the source file."
    )


def sanity_check_outputs(tb: Table, tb_countries: Table, totals_input: Table) -> None:
    assert not tb.duplicated(subset=["producer_country", "consumer_country", "commodity_group", "year"]).any(), (
        "Duplicated rows after aggregating commodities into commodity groups."
    )
    assert (tb["deforestation_risk"] >= 0).all(), "Negative deforestation area after aggregation."
    # Summing commodities into commodity groups must not change the yearly world totals.
    totals_output = tb.groupby("year", observed=True)[VALUE_COLUMNS].sum()
    assert np.allclose(
        totals_output.to_numpy(dtype=float), totals_input.loc[totals_output.index].to_numpy(dtype=float), rtol=1e-9
    ), "Yearly world totals changed after aggregating commodities into commodity groups."
    assert set(tb["producer_country"]) <= set(tb_countries["country"]), "Producer country missing from countries table."
    assert set(tb["consumer_country"]) <= set(tb_countries["country"]), "Consumer country missing from countries table."


def build_countries_table(tb: Table) -> Table:
    """One row per country with the source's ISO code and region, checked to be the same whether
    the country appears as producer or consumer."""
    producers = tb[["producer_country", "producer_iso_code", "producer_region"]].rename(
        columns={"producer_country": "country", "producer_iso_code": "iso_code", "producer_region": "region"}
    )
    consumers = tb[["consumer_country", "consumer_iso_code", "consumer_region"]].rename(
        columns={"consumer_country": "country", "consumer_iso_code": "iso_code", "consumer_region": "region"}
    )
    tb_countries = producers.drop_duplicates()
    tb_countries = tb_countries.merge(consumers.drop_duplicates(), how="outer")
    assert not tb_countries["country"].duplicated().any(), (
        "A country has more than one ISO code or region in the source file."
    )
    for column in ["iso_code", "region"]:
        tb_countries[column] = tb_countries[column].astype(str).copy_metadata(tb["producer_region"])
    return tb_countries


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("deforestation_embedded_in_trade")
    tb = ds_meadow.read("deforestation_embedded_in_trade", safe_types=False)

    sanity_check_inputs(tb)

    # Convert emissions from million tonnes to tonnes of CO2.
    tb["deforestation_emissions"] *= 1e6

    totals_input = tb.groupby("year", observed=True)[VALUE_COLUMNS].sum()

    #
    # Process data.
    #
    for column in ["producer_country", "consumer_country"]:
        tb = paths.regions.harmonize_names(
            tb, country_col=column, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
        )

    tb_countries = build_countries_table(tb)

    # Sum the source's 161 commodities into its 9 commodity groups. The consumption column
    # (domestic, regional or international) follows from the two countries and their regions.
    tb = (
        tb.groupby(["producer_country", "consumer_country", "commodity_group", "year"], observed=True)[VALUE_COLUMNS]
        .sum()
        .reset_index()
    )

    sanity_check_outputs(tb, tb_countries, totals_input)

    tb = tb.format(["producer_country", "consumer_country", "commodity_group", "year"])
    tb_countries = tb_countries.format(["country"], short_name="countries")

    #
    # Save outputs.
    #
    # Keep float64 precision: repacking would store the values as float32.
    ds_garden = paths.create_dataset(tables=[tb, tb_countries], repack=False)
    ds_garden.save()
