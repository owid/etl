import json
import re
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import structlog
from owid.catalog import Source, Table, VariableMeta
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

    #
    # Process data.
    #
    tb_meadow = ds_meadow["wdi"]
    df = pd.DataFrame(ds_meadow["wdi"]).reset_index()

    df = geo.harmonize_countries(
        df=df,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    ).set_index(
        ["country", "year"], verify_integrity=True
    )  # type: ignore

    df_cust = mk_custom_entities(df)
    assert all([col in df.columns for col in df_cust.columns])
    df = pd.concat([df, df_cust], axis=0)

    tb_garden = Table(df, metadata=tb_meadow.metadata)

    log.info("wdi.add_variable_metadata")
    tb_garden = add_variable_metadata(tb_garden, ds_meadow.metadata.sources[0])

    tb_omm = mk_omms(tb_garden)
    tb_garden = tb_garden.join(tb_omm, how="outer")

    # add empty strings to all columns without units
    for col in tb_garden.columns:
        if tb_garden[col].metadata.unit is None:
            tb_garden[col].metadata.unit = ""

    # validate that all columns have title
    for col in tb_garden.columns:
        assert tb_garden[col].metadata.title is not None, 'Variable "{}" has no title'.format(col)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wdi.end")


def mk_omms(table: Table) -> Table:
    """calculates custom variables (aka "owid-maintained metrics")"""
    df = pd.DataFrame(table)
    orig_df_shape = df.shape[0]
    omms = []
    tb_omm = Table()

    # for convenience, uses the same generic "Our World in Data based on..."
    # source for all OMMs created here
    source = deepcopy(table["sp_pop_totl"].metadata.sources[0])
    omm_source = Source(
        name="Our World in Data based on World Bank",
        description=None,
        url=source.url,
        source_data_url=source.source_data_url,
        owid_data_url=source.owid_data_url,
        date_accessed=source.date_accessed,
        publication_date=source.publication_date,
        publication_year=source.publication_year,
        published_by=f"Our World in Data based on {source.published_by}",
    )

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
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[urb_pop_code].metadata.title}"'
            " and"
            f' "{table[slums_pct_code].metadata.title}"'
            "\n\n----\n"
            f"{table[urb_pop_code].metadata.title}:"
            f" {table[urb_pop_code].metadata.description}"
            "\n\n----\n"
            f"{table[slums_pct_code].metadata.title}:"
            f" {table[slums_pct_code].metadata.description}"
        ),
        sources=[omm_source],
        unit="",
        short_unit="",
        display={},
        additional_info=None,
    )

    tb_omm[omm_urb_pop_nonslum].metadata = VariableMeta(
        title="Urban population not living in slums",
        description=(
            "Total urban population not living in slums. This variable is calculated"
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[urb_pop_code].metadata.title}"'
            " and"
            f' "{table[slums_pct_code].metadata.title}"'
            "\n\n----\n"
            f"{table[urb_pop_code].metadata.title}:"
            f" {table[urb_pop_code].metadata.description}"
            "\n\n----\n"
            f"{table[slums_pct_code].metadata.title}:"
            f" {table[slums_pct_code].metadata.description}"
        ),
        sources=[omm_source],
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
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[service_exp_code].metadata.title}"'
            " and"
            f' "{table[goods_exp_code].metadata.title}"'
            "\n\n----\n"
            f"{table[service_exp_code].metadata.title}:"
            f" {table[service_exp_code].metadata.description}"
            "\n\n----\n"
            f"{table[goods_exp_code].metadata.title}:"
            f" {table[goods_exp_code].metadata.description}"
        ),
        sources=[omm_source],
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
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[merch_code].metadata.title}"'
            " and"
            f' "{table[gdp_code].metadata.title}"'
            "\n\n----\n"
            f"{table[merch_code].metadata.title}:"
            f" {table[merch_code].metadata.description}"
            "\n\n----\n"
            f"{table[gdp_code].metadata.title}:"
            f" {table[gdp_code].metadata.description}"
        ),
        sources=[omm_source],
        unit="% of GDP",
        short_unit="%",
        display={},
        additional_info=None,
    )

    tb_omm[omm_goods_code].metadata = VariableMeta(
        title="Goods exports as a share of GDP",
        description=(
            "Goods exports as a share of GDP. This variable is calculated"
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[goods_code].metadata.title}"'
            " and"
            f' "{table[gdp_code].metadata.title}"'
            "\n\n----\n"
            f"{table[goods_code].metadata.title}:"
            f" {table[goods_code].metadata.description}"
            "\n\n----\n"
            f"{table[gdp_code].metadata.title}:"
            f" {table[gdp_code].metadata.description}"
        ),
        sources=[omm_source],
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
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[tax_rev_share_gdp_code].metadata.title}"'
            " and"
            f' "{table[gdp_percap_code].metadata.title}"'
            "\n\n----\n"
            f"{table[tax_rev_share_gdp_code].metadata.title}:"
            f" {table[tax_rev_share_gdp_code].metadata.description}"
            "\n\n----\n"
            f"{table[gdp_percap_code].metadata.title}:"
            f" {table[gdp_percap_code].metadata.description}"
        ),
        sources=[omm_source],
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
            f" by Our World in Data based on the following variables from {source.published_by}:"
            f' "{table[net_savings_code].metadata.title}"'
            " and"
            f' "{table[pop_code].metadata.title}"'
            "\n\n----\n"
            f"{table[net_savings_code].metadata.title}:"
            f" {table[net_savings_code].metadata.description}"
            "\n\n----\n"
            f"{table[pop_code].metadata.title}:"
            f" {table[pop_code].metadata.description}"
        ),
        sources=[omm_source],
        unit=table[net_savings_code].metadata.unit,
        short_unit=table[net_savings_code].metadata.short_unit,
        display={},
        additional_info=None,
    )

    assert orig_df_shape == df.shape[0], "unexpected behavior: original df changed shape in `mk_omms(...)`"
    return tb_omm[omms]


def mk_custom_entities(df: pd.DataFrame) -> pd.DataFrame:
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

    return df_vars.set_index("indicator_code")


def add_variable_metadata(table: Table, ds_source: Source) -> Table:
    var_codes = table.columns.tolist()

    df_vars = load_variable_metadata()

    clean_source_mapping = load_clean_source_mapping()

    table.update_metadata_from_yaml(paths.metadata_path, "wdi", extra_variables="ignore")

    # construct metadata for each variable
    for var_code in var_codes:
        var = df_vars.loc[var_code].to_dict()

        # retrieve clean source name, then construct source.
        source_raw_name = var["source"]
        clean_source = clean_source_mapping.get(source_raw_name)
        assert clean_source, f'`rawName` "{source_raw_name}" not found in wdi.sources.json'
        source = Source(
            name=clean_source["name"],
            description=None,
            url=ds_source.url,
            source_data_url=ds_source.source_data_url,
            date_accessed=str(ds_source.date_accessed),
            publication_date=str(ds_source.publication_date),
            publication_year=ds_source.publication_year,
            published_by=ds_source.name,
        )

        table[var_code].metadata.description = create_description(var)
        table[var_code].metadata.sources = [source]

    if not all([len(table[var_code].sources) == 1 for var_code in var_codes]):
        missing = [var_code for var_code in var_codes if len(table[var_code].sources) != 1]
        raise RuntimeError(
            "Expected each variable code to have one source, but the following variables "
            f"do not: {missing}. Are the source names for these variables "
            "missing from `wdi.sources.json`?"
        )

    return table


def load_clean_source_mapping() -> Dict[str, Dict[str, str]]:
    # TODO: say something about how it was created
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
