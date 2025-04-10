import json
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

import owid.catalog.processing as pr
import pandas as pd
import structlog
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    tb_meadow = ds_meadow["wdi"].reset_index()

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
    tb_garden = add_variable_metadata(tb_garden)

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

    ####################################################################################################################

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

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


def load_variable_metadata() -> pd.DataFrame:
    snap = paths.load_snapshot()
    zf = zipfile.ZipFile(snap.path)
    df_vars = pd.read_csv(zf.open("WDISeries.csv"))

    df_vars.dropna(how="all", axis=1, inplace=True)
    df_vars.columns = df_vars.columns.map(underscore)
    df_vars.rename(columns={"series_code": "indicator_code"}, inplace=True)
    df_vars["indicator_code"] = df_vars["indicator_code"].apply(underscore)

    df_vars["indicator_name"] = df_vars["indicator_name"].str.replace(r"\s+", " ", regex=True)

    # remove non-breaking spaces
    df_vars.source = df_vars.source.str.replace("\xa0", " ").replace("\u00a0", " ")

    return df_vars.set_index("indicator_code")


def add_variable_metadata(table: Table) -> Table:
    var_codes = table.columns.tolist()

    df_vars = load_variable_metadata()

    clean_source_mapping = load_clean_source_mapping()

    table.update_metadata_from_yaml(paths.metadata_path, "wdi", extra_variables="ignore")

    # construct metadata for each variable
    for var_code in var_codes:
        var = df_vars.loc[var_code].to_dict()

        # raw source name, can be very long and can be sometimes full citation
        source_raw_name = var["source"]

        # load metadata created from raw source name
        clean_source = clean_source_mapping.get(source_raw_name)
        assert clean_source, f'`rawName` "{source_raw_name}" not found in wdi.sources.json. Run update_metadata.ipynb or check non-breaking spaces.'

        # create an origin with WDI source name as producer
        table[var_code].m.origins[0].producer = clean_source["name"]

        table[var_code].m.description_from_producer = create_description(var)

    if not all([len(table[var_code].origins) == 1 for var_code in var_codes]):
        missing = [var_code for var_code in var_codes if len(table[var_code].origins) != 1]
        raise RuntimeError(
            "Expected each variable code to have one origin, but the following variables "
            f"do not: {missing}. Are the source names for these variables "
            "missing from `wdi.sources.json`?"
        )

    return table


def load_clean_source_mapping() -> Dict[str, Dict[str, str]]:
    # The mapping was generated by update_metadata.ipynb notebook
    with open(Path(__file__).parent / "wdi.sources.json", "r") as f:
        sources = json.load(f)
        source_mapping = {source["rawName"]: source for source in sources}
        assert len(sources) == len(source_mapping)
    return source_mapping


def create_description(var: Dict[str, Any]) -> Optional[str]:
    desc = ""
    if pd.notnull(var["long_definition"]) and len(var["long_definition"].strip()) > 0:
        desc += var["long_definition"]
    elif pd.notnull(var["short_definition"]) and len(var["short_definition"].strip()) > 0:
        desc += var["short_definition"]

    if pd.notnull(var["limitations_and_exceptions"]) and len(var["limitations_and_exceptions"].strip()) > 0:
        desc += f'\n\nLimitations and exceptions: {var["limitations_and_exceptions"]}'

    if (
        pd.notnull(var["statistical_concept_and_methodology"])
        and len(var["statistical_concept_and_methodology"].strip()) > 0
    ):
        desc += f'\n\nStatistical concept and methodology: {var["statistical_concept_and_methodology"]}'

    # retrieves additional source info, if it exists.
    if pd.notnull(var["notes_from_original_source"]) and len(var["notes_from_original_source"].strip()) > 0:
        desc += f'\n\nNotes from original source: {var["notes_from_original_source"]}'

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
