"""

UPDATE INSTRUCTIONS:

It's easier to do it in two steps:

1. Run indicator upgrader for:
    - GDP per capita, PPP (constant 2021 international $)
    - Current health expenditure per capita, PPP (current international $)

   There are tons of charts using these indicators, if a couple of them look good, it's safe to approve them all.

2. Run indicator upgrader for the rest
    - Auto-approve all charts with no changes
    - Manually review the rest


NEXT UPDATE:
- Indicator `it_net_user_zs` (chart 755) still uses old version because the new one doesn't have regional aggregates.
  Is it still the case? If we calculate them ourselves, do they look ok?
- Write a script to auto-approve charts with no changes.
- We have a function for cleaning up source names https://github.com/owid/etl/pull/4980/files#diff-634c1b07a87794d87af9fbf6c92cae09a5a78caa83dd3a2a27505274802e45c5R187
  Should we replace update_metadata.ipynb with it?
- "dataPublisherSource" is no longer returned by WDI. Remove it if that's the case.
- Indicator metadata from downloaded ZIP file is outdated and we have to fetch metadata from API. Have they solved
    this problem? If yes, we can go back to ZIP file only.
- Check old WDI version and try to switch their charts to new indicators and archive them.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional

import owid.catalog.processing as pr
import pandas as pd
import requests
import structlog
from joblib import Memory
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import CACHE_DIR

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define GDP/GDP per capita indicators in current US$ and their counterpart in constant LCU
GDP_INDICATORS = {"ny_gdp_mktp_cd": "ny_gdp_mktp_kn", "ny_gdp_pcap_cd": "ny_gdp_pcap_kn"}

# Define base year to calculate constant 2021 US$ GDPs to compare with constant 2021 int-$ GDPs
BASE_YEAR_FOR_CONSTANT_USD_GDP = 2021

# Define regions for population weighted aggregations of GDP
REGIONS = [
    "Asia",
    "Europe",
    "Africa",
    "North America",
    "South America",
    "Oceania",
    "World",
]

REGIONS_AGG = [
    "South Asia (WB)",
    "North America (WB)",
    "Sub-Saharan Africa (WB)",
    "East Asia and Pacific (WB)",
    "Middle East and North Africa (WB)",
    "Europe and Central Asia (WB)",
    "Latin America and Caribbean (WB)",
    "World",
]

# Define the fraction of allowed NaNs per year for the population weighted aggregations
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run() -> None:
    log.info("wdi.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset()
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    tb_meadow = ds_meadow.read("wdi", safe_types=False)
    tb_metadata = ds_meadow.read("wdi_metadata", safe_types=False)

    tb = geo.harmonize_countries(
        df=tb_meadow,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    ).set_index(["country", "year"], verify_integrity=True)  # type: ignore

    tb_cust = mk_custom_entities(tb)
    assert all([col in tb.columns for col in tb_cust.columns])
    tb = pd.concat([tb, tb_cust], axis=0).copy_metadata(tb)  # type: ignore

    tb_garden = tb

    log.info("wdi.add_variable_metadata")
    tb_garden = add_variable_metadata(tb_garden, tb_metadata)

    tb_omm = mk_omms(tb_garden)
    tb_garden = tb_garden.join(tb_omm, how="outer")

    # add empty strings to all columns without units
    for col in tb_garden.columns:
        if tb_garden[col].metadata.unit is None:
            tb_garden[col].metadata.unit = ""

    # validate that all columns have title
    for col in tb_garden.columns:
        assert tb_garden[col].metadata.title is not None, 'Variable "{}" has no title'.format(col)

    # add armed personnel as share of population
    tb_garden = add_armed_personnel_as_share_of_population(tb_garden, ds_population)

    # add regions to remittance data
    tb_garden = add_regions_to_remittance_data(tb_garden, ds_regions, ds_income_groups)

    # Adjust GDP indicators in current US$ to constant US$ using the growth of the same indicator in LCU
    for gdp_current_usd, gdp_constant_lcu in GDP_INDICATORS.items():
        tb_garden = adjust_current_to_constant_usd(
            tb=tb_garden,
            indicator_current_usd=gdp_current_usd,
            indicator_constant_lcu=gdp_constant_lcu,
            base_year=BASE_YEAR_FOR_CONSTANT_USD_GDP,
            ds_regions=ds_regions,
            ds_population=ds_population,
        )

    tb_garden = add_energy_access_variables(tb_garden)

    tb_garden = add_patents_articles_per_million_people(tb_garden)

    # NOTE: This version of WDI doesn't have regional aggregates for internet users (it_net_user_zs).
    #  I tried to calculate them myself, but some large countries such as India have missing values and
    #  the time-series looks jagged. It's better to wait for the next WDI release and hope
    #  they'll bring back aggregations.
    #
    # Add population-weighted regional aggregations for internet users
    # tb_garden = (
    #     paths.region_aggregator(regions=REGIONS_AGG, aggregations={"it_net_user_zs": "weighted_by_population"})
    #     .add_aggregates(
    #         tb_garden.reset_index(),
    #     )
    #     .format(["country", "year"])
    # )

    # Add population-weighted regional aggregations for sh_h2o_basw_zs and sh_sta_bass_zs
    tb_garden = (
        paths.region_aggregator(
            regions=["World"],
            aggregations={"sh_h2o_basw_zs": "weighted_by_population", "sh_sta_bass_zs": "weighted_by_population"},
        )
        .add_aggregates(
            tb_garden.reset_index(),
        )
        .format(["country", "year"])
    )

    ####################################################################################################################

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wdi.end")


def add_regions_to_remittance_data(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    """
    Add regions to remittance data, if more than 75% of remittance volume sent/ received is covered by cost data.

    notes for indicators:
    - si_rmt_cost_ib_zs: % cost of receiving remittances (inbound)
    - si_rmt_cost_ob_zs: % cost of sending remittances (outbound)
    - bx_trf_pwkr_cd_dt: total remittances received by country
    - bm_trf_pwkr_cd_dt: total remittances sent by country
    """

    tb = tb.reset_index()

    # create a copy so other indicators are not affected
    regions_tb = tb.copy()

    # create new columns for total remittances (only for countries where remittance cost is available)
    # this is needed to calculate share of remittance volume covered by cost data
    regions_tb["total_received_remittances"] = regions_tb["bx_trf_pwkr_cd_dt"].where(
        regions_tb["si_rmt_cost_ib_zs"].notna()
    )
    regions_tb["total_sent_remittances"] = regions_tb["bm_trf_pwkr_cd_dt"].where(
        regions_tb["si_rmt_cost_ob_zs"].notna()
    )

    # calculate total cost of remittance for each country
    regions_tb["total_cost_of_receiving_remittances"] = (
        regions_tb["si_rmt_cost_ib_zs"] * regions_tb["total_received_remittances"]
    )
    regions_tb["total_cost_of_sending_remittances"] = (
        regions_tb["si_rmt_cost_ob_zs"] * regions_tb["total_sent_remittances"]
    )

    # aggregation for regions
    agg = {
        "total_cost_of_receiving_remittances": "sum",
        "total_cost_of_sending_remittances": "sum",
        "total_received_remittances": "sum",
        "total_sent_remittances": "sum",
        "bx_trf_pwkr_cd_dt": "sum",
        "bm_trf_pwkr_cd_dt": "sum",
    }

    # add regions to table
    regions_tb = geo.add_regions_to_table(
        regions_tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        aggregations=agg,
        min_num_values_per_year=1,
    )

    # calculate cost of remittances per region
    regions_tb["calc_cost_received_for_regions"] = (
        regions_tb["total_cost_of_receiving_remittances"] / regions_tb["total_received_remittances"]
    )
    regions_tb["calc_cost_sent_for_regions"] = (
        regions_tb["total_cost_of_sending_remittances"] / regions_tb["total_sent_remittances"]
    )

    # calculate share of remittances covered by cost
    regions_tb["perc_covered_by_cost_received"] = (
        regions_tb["total_received_remittances"] / regions_tb["bx_trf_pwkr_cd_dt"]
    )
    regions_tb["perc_covered_by_cost_sent"] = regions_tb["total_sent_remittances"] / regions_tb["bm_trf_pwkr_cd_dt"]

    # only keep cost for regions if >75% of remittance volumne sent/ received is covered by cost
    regions_tb["si_rmt_cost_ib_zs"] = regions_tb["calc_cost_received_for_regions"].where(
        regions_tb["perc_covered_by_cost_received"] > 0.75
    )
    regions_tb["si_rmt_cost_ob_zs"] = regions_tb["calc_cost_sent_for_regions"].where(
        regions_tb["perc_covered_by_cost_sent"] > 0.75
    )

    col_to_replace = [
        "si_rmt_cost_ib_zs",
        "si_rmt_cost_ob_zs",
        "bx_trf_pwkr_cd_dt",
        "bm_trf_pwkr_cd_dt",
    ]

    col_rest = [col for col in tb.columns if col not in col_to_replace]

    tb = pr.merge(tb[col_rest], regions_tb[col_to_replace + ["country", "year"]], on=["country", "year"], how="outer")

    tb = tb.format(["country", "year"])

    return tb


def mk_omms(table: Table) -> Table:
    """calculates custom variables (aka "owid-maintained metrics")"""
    df = pd.DataFrame(table)
    orig_df_shape = df.shape[0]
    omms = []
    tb_omm = Table()

    # for convenience, uses the same generic "Our World in Data based on..."
    # source for all OMMs created here
    omm_origin = table["sp_pop_totl"].metadata.origins[0].copy()

    # rewrite "World Development Indicators" to "Our World in Data based on World Bank"
    omm_origin.citation_full = f"{omm_origin.producer} - {omm_origin.citation_full}"

    published_by = omm_origin.producer

    # omm: urban population living in slums
    urb_pop_code = "sp_urb_totl"  # Urban population
    slums_pct_code = "en_pop_slum_ur_zs"  # Population living in slums (% of urban population)
    omm_urb_pop_slum = "omm_urb_pop_slum"
    omm_urb_pop_nonslum = "omm_urb_pop_nonslum"
    assert df[slums_pct_code].min() >= 0.0 and df[slums_pct_code].max() <= 100.0 and df[slums_pct_code].max() > 1.0
    tb_omm[omm_urb_pop_slum] = df[urb_pop_code].astype(float).multiply(df[slums_pct_code].divide(100)).round(0)
    tb_omm[omm_urb_pop_nonslum] = df[urb_pop_code].astype(float) - tb_omm[omm_urb_pop_slum]
    omms += [omm_urb_pop_slum, omm_urb_pop_nonslum]
    tb_omm[omm_urb_pop_slum].metadata = VariableMeta(
        title="Urban population living in slums",
        description=(
            "Total urban population living in slums. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[urb_pop_code].metadata.title}"'
            " and"
            f' "{table[slums_pct_code].metadata.title}"'
            "\n\n----\n"
            f"{table[urb_pop_code].metadata.title}:"
            f" {table[urb_pop_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[slums_pct_code].metadata.title}:"
            f" {table[slums_pct_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit="",
        short_unit="",
        display={},
        additional_info=None,
    )

    tb_omm[omm_urb_pop_nonslum].metadata = VariableMeta(
        title="Urban population not living in slums",
        description=(
            "Total urban population not living in slums. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[urb_pop_code].metadata.title}"'
            " and"
            f' "{table[slums_pct_code].metadata.title}"'
            "\n\n----\n"
            f"{table[urb_pop_code].metadata.title}:"
            f" {table[urb_pop_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[slums_pct_code].metadata.title}:"
            f" {table[slums_pct_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit="",
        short_unit="",
        display={},
        additional_info=None,
    )

    # omm: services exports as % of goods+services exports
    service_exp_code = "bx_gsr_nfsv_cd"
    goods_exp_code = "bx_gsr_mrch_cd"
    omm_service_pct_code = "omm_share_service_exports"
    tb_omm[omm_service_pct_code] = (
        (df[service_exp_code] / (df[service_exp_code] + df[goods_exp_code])).multiply(100).round(2)
    )
    omms.append(omm_service_pct_code)
    tb_omm[omm_service_pct_code].metadata = VariableMeta(
        title="Share of services in total goods and services exports",
        description=(
            "Service exports as a share of total exports of goods and services. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[service_exp_code].metadata.title}"'
            " and"
            f' "{table[goods_exp_code].metadata.title}"'
            "\n\n----\n"
            f"{table[service_exp_code].metadata.title}:"
            f" {table[service_exp_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[goods_exp_code].metadata.title}:"
            f" {table[goods_exp_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit="% of goods and services exports",
        short_unit="%",
        display={},
        additional_info=None,
    )

    # merchandise exports as % of GDP; goods exports as % of GDP
    merch_code = "tx_val_mrch_cd_wt"
    goods_code = "bx_gsr_mrch_cd"
    gdp_code = "ny_gdp_mktp_cd"
    omm_merch_code = "omm_merch_exp_share_gdp"
    omm_goods_code = "omm_goods_exp_share_gdp"
    tb_omm[omm_merch_code] = df[merch_code].divide(df[gdp_code]).multiply(100).round(2)
    tb_omm[omm_goods_code] = df[goods_code].divide(df[gdp_code]).multiply(100).round(2)
    omms += [omm_merch_code, omm_goods_code]

    tb_omm[omm_merch_code].metadata = VariableMeta(
        title="Merchandise exports as a share of GDP",
        description=(
            "Merchandise exports as a share of GDP. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[merch_code].metadata.title}"'
            " and"
            f' "{table[gdp_code].metadata.title}"'
            "\n\n----\n"
            f"{table[merch_code].metadata.title}:"
            f" {table[merch_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[gdp_code].metadata.title}:"
            f" {table[gdp_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit="% of GDP",
        short_unit="%",
        display={},
        additional_info=None,
    )

    tb_omm[omm_goods_code].metadata = VariableMeta(
        title="Goods exports as a share of GDP",
        description=(
            "Goods exports as a share of GDP. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[goods_code].metadata.title}"'
            " and"
            f' "{table[gdp_code].metadata.title}"'
            "\n\n----\n"
            f"{table[goods_code].metadata.title}:"
            f" {table[goods_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[gdp_code].metadata.title}:"
            f" {table[gdp_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit="% of GDP",
        short_unit="%",
        display={},
        additional_info=None,
    )

    # tax revenue (PPP)
    tax_rev_share_gdp_code = "gc_tax_totl_gd_zs"
    gdp_percap_code = "ny_gdp_pcap_pp_cd"
    omm_tax_rev_code = "omm_tax_rev_percap"
    assert (
        df[tax_rev_share_gdp_code].min() >= 0.0
        and df[tax_rev_share_gdp_code].quantile(0.995)
        <= 100.0  # timor has > 100 tax revenue as a % of GDP for 2010-2012
        and df[tax_rev_share_gdp_code].max() > 1.0
    )
    tb_omm[omm_tax_rev_code] = df[gdp_percap_code].multiply(df[tax_rev_share_gdp_code].divide(100)).round(2)
    omms.append(omm_tax_rev_code)

    tb_omm[omm_tax_rev_code].metadata = VariableMeta(
        title="Tax revenues per capita (current international $)",
        description=(
            "Tax revenues per capita, expressed in current international $. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[tax_rev_share_gdp_code].metadata.title}"'
            " and"
            f' "{table[gdp_percap_code].metadata.title}"'
            "\n\n----\n"
            f"{table[tax_rev_share_gdp_code].metadata.title}:"
            f" {table[tax_rev_share_gdp_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[gdp_percap_code].metadata.title}:"
            f" {table[gdp_percap_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit=table[gdp_percap_code].metadata.unit,
        short_unit=table[gdp_percap_code].metadata.short_unit,
        display={},
        additional_info=None,
    )

    # adjusted net savings per capita
    net_savings_code = "ny_adj_svnx_cd"
    pop_code = "sp_pop_totl"
    omm_net_savings_code = "omm_net_savings_percap"
    tb_omm[omm_net_savings_code] = df[net_savings_code].divide(df[pop_code].astype(float)).round(2)
    omms.append(omm_net_savings_code)

    title = re.sub("Adjusted net savings", "Adjusted net savings per capita", table[net_savings_code].metadata.title)
    assert "per capita" in title
    tb_omm[omm_net_savings_code].metadata = VariableMeta(
        title=title,
        description=(
            "Adjusted net savings per capita (excluding particulate emission"
            " damage), expressed in current US$. This variable is calculated"
            f" by Our World in Data based on the following variables from {published_by}:"
            f' "{table[net_savings_code].metadata.title}"'
            " and"
            f' "{table[pop_code].metadata.title}"'
            "\n\n----\n"
            f"{table[net_savings_code].metadata.title}:"
            f" {table[net_savings_code].metadata.description or ''}"
            "\n\n----\n"
            f"{table[pop_code].metadata.title}:"
            f" {table[pop_code].metadata.description or ''}"
        ),
        origins=[omm_origin],
        unit=table[net_savings_code].metadata.unit,
        short_unit=table[net_savings_code].metadata.short_unit,
        display={},
        additional_info=None,
    )

    assert orig_df_shape == df.shape[0], "unexpected behavior: original df changed shape in `mk_omms(...)`"
    return tb_omm[omms]


def mk_custom_entities(df: Table) -> pd.DataFrame:
    """constructs observations for custom entities, to be appended to existing df

    e.g. poverty headcount for "World (excluding China)"
    """
    # poverty headcount for world (excluding china)
    pop_code = "sp_pop_totl"
    pov_share_code = "si_pov_dday"
    omm_pov_count_code = "omm_pov_count"
    assert df[pov_share_code].min() >= 0.0 and df[pov_share_code].max() <= 100.0 and df[pov_share_code].max() > 1.0
    s_temp = df[pop_code].astype(float).multiply(df[pov_share_code].divide(100)).round(0)
    s_temp.name = omm_pov_count_code

    res = []
    country = "World (excluding China)"
    df_temp = pd.merge(
        df[[pop_code]].query('country in ["World", "China"]'),
        s_temp.to_frame().query('country in ["World", "China"]'),
        left_index=True,
        right_index=True,
    ).unstack("country")
    for yr, gp in df_temp.groupby("year"):
        world_exc_pop = (gp[(pop_code, "World")] - gp[(pop_code, "China")]).squeeze()
        world_exc_pov = (gp[(omm_pov_count_code, "World")] - gp[(omm_pov_count_code, "China")]).squeeze()
        world_exc_share_pov = (world_exc_pov / world_exc_pop) * 100
        if pd.notnull(world_exc_share_pov):
            res.append([country, yr, world_exc_share_pov])

    df_cust = pd.DataFrame(res, columns=["country", "year", pov_share_code]).set_index(["country", "year"])
    df_cust[pov_share_code] = df_cust[pov_share_code].round(1)
    return df_cust


@memory.cache
def _fetch_metadata_for_indicator(indicator_code: str) -> Dict[str, str]:
    indicator_code = indicator_code.replace("_", ".").upper()
    api_url = f"https://api.worldbank.org/v2/indicator/{indicator_code}?format=json"
    log.info("wdi.fetch_metadata", indicator_code=indicator_code)
    js = requests.get(api_url).json()

    # Metadata not available for indicators such as PER.SI.ALLSI.COV.Q3.TOT
    if len(js) < 2:
        raise ValueError(f"Metadata not available for indicator {indicator_code}")

    d = js[1]
    assert len(d) == 1
    d = d[0]

    # There might be more fields, but we don't use them
    return {
        "indicator_code": indicator_code,
        "indicator_name": d.pop("name"),
        "unit": d.pop("unit"),
        "source": d.pop("sourceOrganization"),
        "topic": d.pop("topics")[0].get("value"),
    }


def load_variable_metadata(df_vars: Table, indicator_codes: list[str]) -> pd.DataFrame:
    # Exclude metadata not in indicator_codes
    df_vars = df_vars[df_vars["series_code"].isin(indicator_codes)].copy()

    # Drop columns that are completely empty
    df_vars.dropna(how="all", axis=1, inplace=True)

    # Check that series_code is unique
    if df_vars["series_code"].duplicated().any():
        duplicated_codes = df_vars[df_vars["series_code"].duplicated(keep=False)]["series_code"].unique()
        raise ValueError(f"Duplicate series_code values found: {duplicated_codes}")

    df_vars.rename(columns={"series_code": "indicator_code"}, inplace=True)

    # Fetch missing indicator metadata
    indicators_without_meta = set(indicator_codes) - set(df_vars["indicator_code"])

    # Add indicators without sources
    indicators_without_meta |= set(df_vars.loc[df_vars.source.isnull(), "indicator_code"])

    # Add indicators without names
    indicators_without_meta |= set(df_vars.loc[df_vars.indicator_name.isnull(), "indicator_code"])

    # Fetch metadata for missing indicators
    # NOTE: this should be ideally in the snapshot, but there are only a few indicators like this so it's
    #   not worth it
    log.info("wdi.missing_metadata", n_indicators=len(indicators_without_meta))
    if indicators_without_meta:
        df_missing = pd.DataFrame([_fetch_metadata_for_indicator(code) for code in indicators_without_meta])
        # Merge missing metadata
        df_vars = pd.concat([df_vars[~df_vars.indicator_code.isin(df_missing.indicator_code)], df_missing])
    # If no missing indicators, no need to merge anything

    # Final checks
    missing_indicator_codes = set(indicator_codes) - set(df_vars["indicator_code"])
    if missing_indicator_codes:
        raise ValueError(f"Missing metadata in WDISeries.csv for the following indicators: {missing_indicator_codes}")

    if df_vars.indicator_name.isnull().any():
        missing_names = df_vars.loc[df_vars.indicator_name.isnull(), "indicator_code"].unique()
        raise RuntimeError(f"Missing names for indicators:\n{missing_names}")

    if df_vars.source.isnull().any():
        missing_sources = df_vars.loc[df_vars.source.isnull(), "indicator_code"].unique()
        raise RuntimeError(f"Missing sources for indicators:\n{missing_sources}")

    # Underscore indicator codes
    df_vars["indicator_code_original"] = df_vars["indicator_code"].copy()
    df_vars["indicator_code"] = df_vars["indicator_code"].apply(underscore)
    df_vars["indicator_name"] = df_vars["indicator_name"].str.replace(r"\s+", " ", regex=True)

    # Remove non-breaking spaces
    df_vars.source = df_vars.source.str.replace("\xa0", " ").replace("\u00a0", " ")

    return df_vars.set_index("indicator_code")


def add_variable_metadata(tb: Table, tb_metadata: Table) -> Table:
    var_codes = tb.columns.tolist()
    indicator_codes = [tb[col].m.title for col in tb.columns]

    df_vars = load_variable_metadata(tb_metadata, indicator_codes)

    missing_var_codes = set(var_codes) - set(df_vars.index)
    if missing_var_codes:
        raise RuntimeError(f"Missing metadata for the following variables: {missing_var_codes}")

    clean_source_mapping = load_clean_source_mapping()

    tb.update_metadata_from_yaml(paths.metadata_path, "wdi", extra_variables="ignore")

    # construct metadata for each variable
    for var_code in var_codes:
        var = df_vars.loc[var_code].to_dict()

        # raw source name, can be very long and can be sometimes full citation
        source_raw_name = var["source"]

        # load metadata created from raw source name
        clean_source = clean_source_mapping.get(source_raw_name)
        assert clean_source, f'{var_code}: `rawName` "{source_raw_name}" not found in wdi.sources.json. Run update_metadata.ipynb or check non-breaking spaces.'

        # create an origin with WDI
        assert len(tb[var_code].m.origins) == 1
        origin = tb[var_code].m.origins[0]

        # Check out this issue for details on the origin: https://github.com/owid/etl/issues/3971#issuecomment-2921855209
        #
        # Example:
        #
        # origin:
        #     producer: Food and Agriculture Organization of the United Nations
        #     title: World Development Indicators (World Bank)
        #     description: |-
        #         The World Development Indicators (WDI) is the primary World Bank collection of development indicators, compiled from officially-recognized international sources. It presents the most current and accurate global development data available, and includes national, regional and global estimates.
        #     url_main: https://data.worldbank.org/indicator/AG.CON.FERT.PT.ZS
        #     citation_full: {rawName}. Indicator AG.CON.FERT.PT.ZS ({url_main}), {title} ({date_published})
        #     attribution: None
        origin.producer = clean_source["name"]
        origin.title = "World Development Indicators"
        origin.url_main = f"https://data.worldbank.org/indicator/{var['indicator_code_original']}"
        origin.citation_full = f"{source_raw_name.rstrip('.')}. Indicator {var['indicator_code_original']} ({origin.url_main}). World Development Indicators - World Bank ({origin.date_published.split('-')[0]}). Accessed on {origin.date_accessed}."

        # set description_from_producer
        tb[var_code].m.description_from_producer = create_description_from_producer(var)

    if not all([len(tb[var_code].origins) == 1 for var_code in var_codes]):
        missing = [var_code for var_code in var_codes if len(tb[var_code].origins) != 1]
        raise RuntimeError(
            "Expected each variable code to have one origin, but the following variables "
            f"do not: {missing}. Are the source names for these variables "
            "missing from `wdi.sources.json`?"
        )

    return tb


def load_clean_source_mapping() -> Dict[str, Dict[str, str]]:
    # The mapping was generated by update_metadata.ipynb notebook
    with open(Path(__file__).parent / "wdi.sources.json", "r") as f:
        sources = json.load(f)
        source_mapping = {source["rawName"]: source for source in sources}
        assert len(sources) == len(source_mapping)
    return source_mapping


def create_description_from_producer(var: Dict[str, Any]) -> Optional[str]:
    desc = ""
    if pd.notnull(var["long_definition"]) and len(var["long_definition"].strip()) > 0:
        desc += var["long_definition"]
    elif pd.notnull(var["short_definition"]) and len(var["short_definition"].strip()) > 0:
        desc += var["short_definition"]

    if pd.notnull(var["limitations_and_exceptions"]) and len(var["limitations_and_exceptions"].strip()) > 0:
        desc += f'\n\n### Limitations and exceptions:\n{var["limitations_and_exceptions"]}'

    if (
        pd.notnull(var["statistical_concept_and_methodology"])
        and len(var["statistical_concept_and_methodology"].strip()) > 0
    ):
        desc += f'\n\n### Statistical concept and methodology:\n{var["statistical_concept_and_methodology"]}'

    ####################################################################################################################
    # I think that the development relevance could also be an interesting field to add to the description_from_producer.
    # For now, I'll include it in this specific indicator (access to electricity), but in the future we can consider adding this field for all indicators.
    if (
        (var["indicator_code_original"] in ["EG.ELC.ACCS.ZS"])
        and pd.notnull(var["development_relevance"])
        and len(var["development_relevance"].strip()) > 0
    ):
        desc += f'\n\n### Development relevance:\n{var["development_relevance"]}'
    ####################################################################################################################

    # retrieves additional source info, if it exists.
    if pd.notnull(var["notes_from_original_source"]) and len(var["notes_from_original_source"].strip()) > 0:
        desc += f'\n\n### Notes from original source:\n{var["notes_from_original_source"]}'

    desc = re.sub(r" *(\n+) *", r"\1", re.sub(r"[ \t]+", " ", desc)).strip()

    if len(desc) == 0:
        return None

    return desc


def add_armed_personnel_as_share_of_population(tb: Table, ds_population: Dataset) -> Table:
    """
    Add armed personnel as share of population.
    Population data is from the OMM population dataset.
    WDI only provides data as a share of total labor force.
    """

    tb = tb.reset_index()

    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=True)

    tb["armed_forces_share_population"] = tb["ms_mil_totl_p1"] / tb["population"] * 100

    # Drop population column
    tb = tb.drop(columns=["population"])

    # Set index again
    tb = tb.format(["country", "year"])

    return tb


def adjust_current_to_constant_usd(
    tb: Table,
    indicator_current_usd: str,
    indicator_constant_lcu: str,
    base_year: int,
    ds_regions: Dataset,
    ds_population: Dataset,
) -> Table:
    """
    Adjust current LCU indicators to constant US$/int-$ using a deflator indicator and the base year.
    The indicators to use have to be in local currency units for this to work.
    The function works equally if the deflator is CPI or a GDP deflator, or any other deflator.

    Available deflator indicators in WDI:

    GDP deflators
        ny_gdp_defl_zs_ad, GDP deflator: linked series (base year varies by country)
        ny_gdp_defl_zs, GDP deflator (base year varies by country)

    CPI deflators
        fp_cpi_totl, Consumer price index (2010 = 100)

    Available conversion indicators in WDI:

    Exchange rates
        pa_nus_fcrf, Official exchange rate (LCU per US$, period average)
        pa_nus_atls, DEC alternative conversion factor (LCU per US$)

    PPP conversion factors
        pa_nus_ppp, PPP conversion factor, GDP (LCU per international $)
        pa_nus_prvt_pp, PPP conversion factor, private consumption (LCU per international $)
    """

    tb = tb.reset_index()

    tb_adjusted = tb[["country", "year"] + [indicator_current_usd] + [indicator_constant_lcu]].copy()

    # Create a new table with the data only for the base year
    tb_base_year = tb_adjusted[tb_adjusted["year"] == base_year].reset_index(drop=True)

    # Merge the two tables
    tb_adjusted = pr.merge(
        tb_adjusted,
        tb_base_year[["country", indicator_current_usd, indicator_constant_lcu]],
        on="country",
        how="left",
        suffixes=("", "_base_year"),
    )

    # Divide the indicator in constant LCU by the indicator in constant LCU in the base year
    tb_adjusted[f"{indicator_constant_lcu}_ratio"] = (
        tb_adjusted[indicator_constant_lcu] / tb_adjusted[f"{indicator_constant_lcu}_base_year"]
    )

    # Adjust the indicator to constant US$
    tb_adjusted[f"{indicator_current_usd}_adjusted"] = (
        tb_adjusted[f"{indicator_current_usd}_base_year"] * tb_adjusted[f"{indicator_constant_lcu}_ratio"]
    )

    tb_adjusted = add_population_weighted_aggregations(
        tb=tb_adjusted,
        indicator=f"{indicator_current_usd}_adjusted",
        ds_regions=ds_regions,
        ds_population=ds_population,
    )

    # Merge the adjusted indicators back to the original table
    tb = pr.merge(
        tb,
        tb_adjusted[["country", "year"] + [f"{indicator_current_usd}_adjusted"]],
        on=["country", "year"],
        how="outer",
    )

    # Reformat again
    tb = tb.format()

    return tb


def add_population_weighted_aggregations(
    tb: Table, indicator: str, ds_regions: Dataset, ds_population: Dataset
) -> Table:
    """
    Add population weighted aggregations for the given indicator.
    This is used together with the adjust_current_to_constant_usd function to calculate constant US$ indicators.
    """
    tb = tb.copy()

    # Remove regions from table
    tb = tb[~tb["country"].isin(REGIONS)].reset_index(drop=True)

    # Add population to the table
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

    # Multiply the indicator by the population to get the weighted value
    tb[f"{indicator}_weighted"] = tb[indicator] * tb["population"]

    # Add regional aggregates for these indicators
    tb_regions = geo.add_regions_to_table(
        tb=tb,
        aggregations={
            f"{indicator}_weighted": "sum",
            "population": "sum",
        },
        regions=REGIONS,
        ds_regions=ds_regions,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Filter only by regions
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].reset_index(drop=True)

    # Divide the weighted indicator by the population to get the weighted average
    tb_regions[indicator] = tb_regions[f"{indicator}_weighted"] / tb_regions["population"]

    # Keep only the columns we need
    tb_regions = tb_regions[["country", "year", indicator]]

    # Merge the regional data back to the original table
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    return tb


def add_energy_access_variables(tb: Table) -> Table:
    """
    Calculate the following energy related variables:

    1. Share of the population without access to electricity
    2. Number of people with access to electricity
    3. Number of people without access to electricity
    4. Share of the population without access to clean cooking fuels
    5. Number of people with access to clean cooking fuels
    6. Number of people without access to clean cooking fuels

    """

    # Add energy access variables
    tb = tb.reset_index()

    tb["eg_elc_accs_zs_without"] = 100 - tb["eg_elc_accs_zs"]
    tb["eg_cft_accs_zs_without"] = 100 - tb["eg_cft_accs_zs"]

    # Calculate number of people with and without access to electricity
    tb["eg_elc_accs_zs_number"] = tb["eg_elc_accs_zs"] / 100 * tb["sp_pop_totl"]
    tb["eg_elc_accs_zs_without_number"] = tb["eg_elc_accs_zs_without"] / 100 * tb["sp_pop_totl"]

    # Calculate number of people with and without access to clean cooking fuels
    tb["eg_cft_accs_zs_number"] = tb["eg_cft_accs_zs"] / 100 * tb["sp_pop_totl"]
    tb["eg_cft_accs_zs_without_number"] = tb["eg_cft_accs_zs_without"] / 100 * tb["sp_pop_totl"]
    tb = tb.format(["country", "year"])

    # Add description from producer to the new indicators (which contains relevant information).
    for indicator in ["eg_elc_accs_zs_without", "eg_elc_accs_zs_number", "eg_elc_accs_zs_without_number"]:
        tb[indicator].metadata.description_from_producer = tb["eg_elc_accs_zs"].metadata.description_from_producer
    for indicator in ["eg_cft_accs_zs_without", "eg_cft_accs_zs_number", "eg_cft_accs_zs_without_number"]:
        tb[indicator].metadata.description_from_producer = tb["eg_cft_accs_zs"].metadata.description_from_producer

    return tb


def add_patents_articles_per_million_people(tb: Table) -> Table:
    """
    Add patents and articles per million people.
    """
    tb = tb.reset_index()

    # Calculate patents and articles per million people
    tb["patents_per_million_people"] = tb["ip_pat_resd"] / tb["sp_pop_totl"] * 1000000
    tb["articles_per_million_people"] = tb["ip_jrn_artc_sc"] / tb["sp_pop_totl"] * 1000000

    tb = tb.format(["country", "year"])
    return tb
