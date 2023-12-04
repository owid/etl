"""

Harmonize country names:

    $ harmonize data/meadow/worldbank_wdi/{version}/wdi/wdi.feather country etl/steps/data/garden/worldbank_wdi/{version}/wdi.country_mapping.json
"""

import json
import re
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import structlog
from owid.catalog import Dataset, Source, Table, VariableMeta
from owid.catalog.utils import underscore
from owid.walden import Catalog

from etl.paths import DATA_DIR

from .variable_matcher import VariableMatcher

COUNTRY_MAPPING_PATH = (Path(__file__).parent / "wdi.country_mapping.json").as_posix()

log = structlog.get_logger()


def run(dest_dir: str) -> None:
    version = Path(__file__).parent.parent.stem
    fname = Path(__file__).parent.stem
    namespace = Path(__file__).parent.parent.parent.stem
    ds_meadow = Dataset((DATA_DIR / f"meadow/{namespace}/{version}/{fname}").as_posix())

    assert len(ds_meadow.table_names) == 1, "Expected meadow dataset to have only one table, but found > 1 table names."
    tb_meadow = ds_meadow[fname]
    df = pd.DataFrame(tb_meadow).reset_index()

    # harmonize entity names
    country_mapping = load_country_mapping()
    excluded_countries = load_excluded_countries()  # noqa: F841
    df = df.query("country not in @excluded_countries").copy()
    assert df["country"].notnull().all()
    countries = df["country"].apply(lambda x: country_mapping.get(x, None))
    if countries.isnull().any():
        missing_countries = [x for x in df["country"].drop_duplicates() if x not in country_mapping]
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            "names; or (b) remove these country names from the raw table."
            f"Raw country names: {missing_countries}"
        )

    df["country"] = countries
    df.set_index(tb_meadow.metadata.primary_key, inplace=True)

    df_cust = mk_custom_entities(df)
    assert all([col in df.columns for col in df_cust.columns])
    df = pd.concat([df, df_cust], axis=0)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = Table(df)
    tb_garden.metadata = tb_meadow.metadata

    tb_garden = add_variable_metadata(tb_garden)

    tb_omm = mk_omms(tb_garden)
    tb_garden2 = tb_garden.join(tb_omm, how="outer")
    tb_garden2.metadata = tb_garden.metadata
    for col in tb_garden2.columns:
        if col in tb_garden:
            tb_garden2[col].metadata = tb_garden[col].metadata
        else:
            tb_garden2[col].metadata = tb_omm[col].metadata

    ds_garden.add(tb_garden2)

    ds_garden.save()


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


def add_variable_metadata(table: Table) -> Table:
    var_codes = table.columns.tolist()

    # retrieves raw data from walden
    version = Path(__file__).parent.parent.stem
    fname = Path(__file__).parent.stem
    namespace = Path(__file__).parent.parent.parent.stem
    walden_ds = Catalog().find_one(namespace=namespace, short_name=fname, version=version)
    local_file = walden_ds.ensure_downloaded()
    zf = zipfile.ZipFile(local_file)
    df_vars = pd.read_csv(zf.open("WDISeries.csv"))
    df_vars.dropna(how="all", axis=1, inplace=True)
    df_vars.columns = df_vars.columns.map(underscore)
    df_vars.rename(columns={"series_code": "indicator_code"}, inplace=True)
    df_vars["indicator_code"] = df_vars["indicator_code"].apply(underscore)
    df_vars = df_vars.query("indicator_code in @var_codes").set_index("indicator_code", verify_integrity=True)

    df_vars["indicator_name"].str.replace(r"\s+", " ", regex=True)
    clean_source_mapping = load_clean_source_mapping()

    # construct metadata for each variable
    vm = VariableMatcher()
    for var_code in var_codes:
        var = df_vars.loc[var_code].to_dict()

        # retrieves unit + display metadata from the most recently updated
        # WDI grapher variable that matches this variable's name
        unit = ""
        short_unit = ""
        display = {}
        grapher_vars = vm.find_grapher_variables(var["indicator_name"])
        if grapher_vars:
            found_unit_metadata = False
            gvar = None
            while len(grapher_vars) and not found_unit_metadata:
                gvar = grapher_vars.pop(0)
                found_unit_metadata = bool(gvar["unit"] or gvar["shortUnit"])

            if found_unit_metadata and gvar:
                if pd.notnull(gvar["unit"]):
                    unit = gvar["unit"]
                if pd.notnull(gvar["shortUnit"]):
                    short_unit = gvar["shortUnit"]
                if pd.notnull(gvar["display"]):
                    display = json.loads(gvar["display"])

                year_regex = re.compile(r"\b([1-2]\d{3})\b")
                regex_res = year_regex.search(var["indicator_name"])
                if regex_res:
                    assert len(regex_res.groups()) == 1
                    year = regex_res.groups()[0]
                    unit = replace_years(unit, year)
                    short_unit = replace_years(short_unit, year)
                    for k in ["name", "unit", "shortUnit"]:
                        if pd.notnull(display.get(k)):
                            display[k] = replace_years(display[k], year)
        else:
            log.warning(
                f"Variable does not match an existing {fname} variable name in the grapher",
                variable_name=var["indicator_name"],
            )

        # retrieve clean source name, then construct source.
        source_raw_name = var["source"]
        clean_source = clean_source_mapping.get(source_raw_name)
        assert clean_source, f'`rawName` "{source_raw_name}" not found in wdi.sources.json'
        assert table[var_code].metadata.to_dict() == {}, (
            f"Expected metadata for variable {var_code} to be empty, but "
            f"metadata is: {table[var_code].metadata.to_dict()}."
        )
        source = Source(
            name=clean_source["name"],
            description=None,
            url=walden_ds.metadata["url"],
            source_data_url=walden_ds.metadata["source_data_url"],
            owid_data_url=walden_ds.metadata["owid_data_url"],
            date_accessed=walden_ds.metadata["date_accessed"],
            publication_date=walden_ds.metadata["publication_date"],
            publication_year=walden_ds.metadata["publication_year"],
            published_by=walden_ds.metadata["name"],
        )

        table[var_code].metadata = VariableMeta(
            title=df_vars.loc[var_code, "indicator_name"],
            description=create_description(var),
            sources=[source],
            unit=unit,
            short_unit=short_unit,
            display=display,
            additional_info=None,
            # licenses=[var['license_type']]
        )

    if not all([len(table[var_code].sources) == 1 for var_code in var_codes]):
        missing = [var_code for var_code in var_codes if len(table[var_code].sources) != 1]
        raise RuntimeError(
            "Expected each variable code to have one source, but the following variables "
            f"do not: {missing}. Are the source names for these variables "
            "missing from `wdi.sources.json`?"
        )

    return table


def load_country_mapping() -> Dict[str, str]:
    with open(COUNTRY_MAPPING_PATH, "r") as f:
        mapping = json.load(f)
        assert isinstance(mapping, dict)
    return mapping


def load_excluded_countries() -> List[str]:
    with open(Path(__file__).parent / "wdi.country_exclude.json", "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def load_clean_source_mapping() -> Dict[str, Dict[str, str]]:
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


def replace_years(s: str, year: Union[int, str]) -> str:
    """replaces all years in string with {year}.

    Example:

        >>> replace_years("GDP (constant 2010 US$)", 2015)
        "GDP (constant 2015 US$)"
    """
    year_regex = re.compile(r"\b([1-2]\d{3})\b")
    s_new = year_regex.sub(str(year), s)
    return s_new
