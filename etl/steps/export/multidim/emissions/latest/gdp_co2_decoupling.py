import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def add_dimensions_to_indicators(tb):
    """Add dimensions metadata to indicators based on column names."""

    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        # Set base metadata
        tb[col].metadata.original_short_name = "decoupling_indicators"
        tb[col].metadata.dimensions = {}

        # Determine metric type (absolute vs per capita)
        if "per_capita" in col or "_pc" in col:
            metric_type = "per_capita"
        else:
            metric_type = "absolute"

        # Determine growth metric type
        if "gdp" in col:
            growth_metric = "gdp"
        elif "gni" in col:
            growth_metric = "gni"
        elif "income" in col:
            growth_metric = "income"
        else:
            growth_metric = "gdp"  # default

        # Determine emissions type
        if "consumption" in col:
            emissions_type = "consumption"
        elif "emissions" in col:
            emissions_type = "production"
        else:
            emissions_type = "production"  # default for non-emissions indicators

        # Set dimensions
        tb[col].metadata.dimensions["metric_type"] = metric_type
        tb[col].metadata.dimensions["growth_metric"] = growth_metric
        tb[col].metadata.dimensions["emissions_type"] = emissions_type

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load collection config
    config = paths.load_collection_config()

    # Load datasets
    wdi_ds = paths.load_dataset("wdi")
    pip_ds = paths.load_dataset("world_bank_pip")
    co2_ds = paths.load_dataset("global_carbon_budget")
    unwpp_ds = paths.load_dataset("un_wpp")

    # WDI GDP data from World Bank
    tb_wdi = wdi_ds.read("wdi")
    tb_wdi = tb_wdi[["country", "year", "ny_gdp_pcap_pp_kd", "ny_gdp_mktp_pp_kd"]].rename(
        columns={
            "ny_gdp_pcap_pp_kd": "gdp_per_capita_ppp",
            "ny_gdp_mktp_pp_kd": "gdp_total_ppp",
        }
    )

    # Poverty and Inequality Platform (PIP) income data from World Bank
    pip_median_col = "median__ppp_version_2021__poverty_line_no_poverty_line__welfare_type_income_or_consumption__table_income_or_consumption_consolidated__survey_comparability_no_spells"
    pip_mean_col = "mean__ppp_version_2021__poverty_line_no_poverty_line__welfare_type_income_or_consumption__table_income_or_consumption_consolidated__survey_comparability_no_spells"
    tb_pip = pip_ds.read("world_bank_pip")
    tb_pip = tb_pip[["country", "year", pip_median_col, pip_mean_col]].rename(
        columns={pip_median_col: "income_median_ppp", pip_mean_col: "income_mean_ppp"}
    )

    # CO2 emissions data from GCP
    tb_co2 = co2_ds.read("global_carbon_budget")
    tb_co2 = tb_co2[
        [
            "country",
            "year",
            "emissions_total",
            "emissions_total_per_capita",
            "consumption_emissions",
            "consumption_emissions_per_capita",
        ]
    ]

    # Population data from UNWPP
    tb_pop = unwpp_ds.read("un_wpp")
    tb_pop = tb_pop[["country", "year", "population__sex_all__age_all__variant_estimates"]].rename(
        columns={"population__sex_all__age_all__variant_estimates": "population"}
    )

    # Merge all datasets
    tb_merged = tb_wdi
    for tb in [tb_pip, tb_co2, tb_pop]:
        tb_merged = tb_merged.merge(tb, on=["country", "year"], how="outer")

    # Clean data - remove rows where all indicators are null
    indicator_cols = [col for col in tb_merged.columns if col not in ["country", "year"]]
    tb_merged = tb_merged.dropna(subset=indicator_cols, how="all")

    # Format as table and add dimensions
    tb_merged = tb_merged.format(short_name="gdp_co2_decoupling")
    tb_merged = add_dimensions_to_indicators(tb_merged)

    # Create the multidimensional collection
    collection = paths.create_collection(
        config=config,
        tb=tb_merged,
        indicator_names=[
            "gdp_per_capita_ppp",
            "gdp_total_ppp",
            "income_median_ppp",
            "income_mean_ppp",
            "emissions_total",
            "emissions_total_per_capita",
            "consumption_emissions",
            "consumption_emissions_per_capita",
        ],
        common_view_config={
            "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
            "hasMapTab": True,
            "tab": "map",
            "originUrl": "https://ourworldindata.org/decoupling",
            "hideAnnotationFieldsInTitle": {"time": True},
            "addCountryMode": "add-country",
        },
    )

    # Save collection
    collection.save()
