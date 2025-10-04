"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load datasets
    wdi_ds = paths.load_dataset("wdi")
    pip_ds = paths.load_dataset("world_bank_pip")
    co2_ds = paths.load_dataset("global_carbon_budget")
    unwpp_ds = paths.load_dataset("un_wpp")

    # WDI GDP & GNI data from World Bank
    tb_wdi = wdi_ds.read("wdi")
    tb_wdi = tb_wdi[
        [
            "country",
            "year",
            "ny_gdp_pcap_pp_kd",  # GDP per capita, PPP (constant 2021 international $)
            "ny_gdp_mktp_pp_kd",  # GDP, PPP (constant 2021 international $)
            "ny_gnp_mktp_pp_kd",  # GNI, PPP (constant 2021 international $)
            "ny_gnp_pcap_pp_kd",  # GNI per capita, PPP (constant 2021 international $)
            "ny_gnp_mktp_kn",  # GNI (LCU constant)
            "ny_gnp_pcap_kn",  # GNI per capita (LCU constant)
        ]
    ].rename(
        columns={
            "ny_gdp_pcap_pp_kd": "gdp_per_capita_ppp",
            "ny_gdp_mktp_pp_kd": "gdp_total_ppp",
            "ny_gnp_mktp_pp_kd": "gni_total_ppp",
            "ny_gnp_pcap_pp_kd": "gni_per_capita_ppp",
            "ny_gnp_mktp_kn": "gni_total_lcu",
            "ny_gnp_pcap_kn": "gni_per_capita_lcu",
        }
    )

    # Poverty and Inequality Platform (PIP) income data from World Bank
    pip_median_col = "median__ppp_version_2021__poverty_line_no_poverty_line__welfare_type_income_or_consumption__table_income_or_consumption_consolidated__survey_comparability_no_spells"
    pip_mean_col = "mean__ppp_version_2021__poverty_line_no_poverty_line__welfare_type_income_or_consumption__table_income_or_consumption_consolidated__survey_comparability_no_spells"
    tb_pip = pip_ds.read("world_bank_pip")
    tb_pip = tb_pip[["country", "year", pip_median_col, pip_mean_col]].rename(
        columns={pip_median_col: "income_median_ppp", pip_mean_col: "income_mean_ppp"}
    )

    # tb_pip["metric"] = "per_capita"

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
    """
    tb_co2 = tb_co2.rename(
        columns={
            "emissions_total": "emission_total_total",
            "consumption_emissions_total": "consumption_emissions_total_total",
        }
    )

    # wide to long (per capita and absolute indicators) - add column 'metric" with values 'absolute' and 'per_capita'
    tb_co2 = tb_co2.wide_to_long(
        stubnames=["emissions_total", "consumption_emissions"], i=["country", "year"], j="metric", sep="_"
    )
    """

    # Population data from UNWPP
    tb_pop = unwpp_ds.read("population")
    tb_pop = tb_pop[["country", "year", "population__sex_all__age_all__variant_estimates"]].rename(
        columns={"population__sex_all__age_all__variant_estimates": "population"}
    )

    # Merge all datasets
    tb_merged = pr.multi_merge(tables=[tb_pop, tb_wdi, tb_pip, tb_co2], on=["country", "year"], how="outer")  # type: ignore

    # Clean data - remove rows where all indicators are null
    indicator_cols = [col for col in tb_merged.columns if col not in ["country", "year"]]
    tb_merged = tb_merged.dropna(subset=indicator_cols, how="all")

    # Format as table and add dimensions
    tb_merged = tb_merged.format(short_name="gdp_co2_decoupling")

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb_merged])

    # Save changes in the new grapher dataset.
    ds_grapher.save()
