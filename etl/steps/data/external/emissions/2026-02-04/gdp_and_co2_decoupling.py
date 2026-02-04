"""This step takes the Global Carbon Budget and GDP data from World Bank's World Development Indicators, and creates a dataset with the changes in emissions and GDP over time.

We already have an interactive chart showing similar data, for per capita GDP and per capita, consumption-based CO2 emissions:
https://ourworldindata.org/grapher/co2-emissions-and-gdp

The data in the current step is not used by any grapher step, but will be used by the following static chart:

The data from this step is used in this static chart:
https://ourworldindata.org/cdn-cgi/imagedelivery/qLq-8BTgXU8yG0N6HnOy8g/f5db1a91-6bde-4430-3c09-e61fd8df9a00/w=2614

"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from WDI, and how to rename them.
COLUMNS_WDI = {
    "country": "country",
    "year": "year",
    # GDP, PPP (constant 2017 international $)
    # "ny_gdp_mktp_pp_kd": "gdp",
    # GDP per capita, PPP (constant 2017 international $)
    "ny_gdp_pcap_pp_kd": "gdp_per_capita",
}

# Columns to select from GCB, and how to rename them.
COLUMNS_GCB = {
    "country": "country",
    "year": "year",
    # "emissions_total": "production_emissions",
    # "emissions_total_per_capita": "production_emissions_per_capita",
    # "consumption_emissions": "consumption_emissions",
    "consumption_emissions_per_capita": "consumption_emissions_per_capita",
    # 'emissions_total_including_land_use_change': "",
    # 'emissions_total_including_land_use_change_per_capita': "",
}

# Minimum percentage change (increase for GDP and decrease in CO2 emissions) for decoupling.
PCT_CHANGE_MIN = 5


def create_changes_table(tb: Table, min_window: int = 1) -> Table:
    """Create a table with percent changes in GDP and emissions for all country-window combinations."""
    # Remove the last year if it only has World data.
    if set(tb[tb["year"] == tb["year"].max()]["country"]) == {"World"}:
        tb = tb[tb["year"] < tb["year"].max()].reset_index(drop=True)

    years = sorted(tb["year"].unique())
    max_window = int(tb["year"].max() - tb["year"].min())

    results = []
    for window_size in range(min_window, max_window + 1):
        for year_min in years:
            year_max = year_min + window_size
            if year_max not in years:
                continue

            tb_start = tb[tb["year"] == year_min][["country", "gdp_per_capita", "consumption_emissions_per_capita"]]
            tb_end = tb[tb["year"] == year_max][["country", "gdp_per_capita", "consumption_emissions_per_capita"]]

            tb_merged = tb_start.merge(tb_end, on="country", suffixes=("_start", "_end"))

            tb_merged["year_min"] = year_min
            tb_merged["year_max"] = year_max
            tb_merged["n_years"] = year_max - year_min
            tb_merged["gdp_per_capita_change"] = (
                (tb_merged["gdp_per_capita_end"] - tb_merged["gdp_per_capita_start"])
                / tb_merged["gdp_per_capita_start"]
                * 100
            )
            tb_merged["consumption_emissions_per_capita_change"] = (
                (
                    tb_merged["consumption_emissions_per_capita_end"]
                    - tb_merged["consumption_emissions_per_capita_start"]
                )
                / tb_merged["consumption_emissions_per_capita_start"]
                * 100
            )

            results.append(
                tb_merged[
                    [
                        "country",
                        "year_min",
                        "year_max",
                        "n_years",
                        "gdp_per_capita_change",
                        "consumption_emissions_per_capita_change",
                    ]
                ]
            )

    return pr.concat(results, ignore_index=True)


def plot_decoupled_countries(tb_change, year_min, year_max, pct_change_min):
    import plotly.express as px

    # Find list of countries that achieved decoupling in the selected window of years.
    countries_decoupled = sorted(
        tb_change[
            (tb_change["year_min"] == year_min)
            & (tb_change["year_max"] == year_max)
            & (tb_change["gdp_per_capita_change"] > pct_change_min)
            & (tb_change["consumption_emissions_per_capita_change"] < -pct_change_min)
        ]["country"].unique()
    )

    # Filter tb_change for the time series: fixed year_min, varying year_max from year_min to year_max.
    tb_plot_base = tb_change[
        (tb_change["country"].isin(countries_decoupled))
        & (tb_change["year_min"] == year_min)
        & (tb_change["year_max"] <= year_max)
    ][["country", "year_max", "gdp_per_capita_change", "consumption_emissions_per_capita_change"]].copy()

    tb_plot_base = tb_plot_base.rename(
        columns={
            "year_max": "year",
            "gdp_per_capita_change": "GDP per capita",
            "consumption_emissions_per_capita_change": "Consumption-based CO2 per capita",
        }
    )

    # Add baseline point at year_min with 0% change.
    tb_plot_base = pr.concat(
        [
            Table(
                {
                    "country": countries_decoupled,
                    "year": year_min,
                    "GDP per capita": 0,
                    "Consumption-based CO2 per capita": 0,
                }
            ),
            tb_plot_base,
        ],
        ignore_index=True,
    )

    # To avoid spurious warnings about mixing units, remove them.
    for column in ["GDP per capita", "Consumption-based CO2 per capita"]:
        tb_plot_base[column].metadata.unit = None
        tb_plot_base[column].metadata.short_unit = None

    for country in countries_decoupled:
        tb_country = tb_plot_base[tb_plot_base["country"] == country].reset_index(drop=True)
        if tb_country.empty:
            continue
        tb_plot = tb_country.melt(id_vars=["country", "year"], var_name="Indicator", value_name="value")
        px.line(
            tb_plot,
            x="year",
            y="value",
            color="Indicator",
            title=f"{country} ({year_min}-{year_max})",
        ).show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load Global Carbon Budget dataset and read its main table.
    ds_gcb = paths.load_dataset("global_carbon_budget")
    tb_gcb = ds_gcb.read("global_carbon_budget")

    # Load WDI dataset, read its main table.
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi.read("wdi")

    #
    # Process data.
    #
    # Select and rename the required variables from GCB.
    tb_gcb = tb_gcb[list(COLUMNS_GCB)].rename(columns=COLUMNS_GCB, errors="raise")

    # Select and rename the required variables from WDI.
    tb_wdi = tb_wdi[list(COLUMNS_WDI)].rename(columns=COLUMNS_WDI, errors="raise")

    # Combine both tables.
    tb = tb_gcb.merge(tb_wdi, on=["country", "year"], how="inner", short_name=paths.short_name)

    # Remove rows with any missing value (we need both GDP and emissions).
    tb = tb.dropna(how="any").reset_index(drop=True)

    # Create a table with all possible combinations of minimum and maximum year, and the change in GDP and emissions of each country.
    tb_change = create_changes_table(tb=tb, min_window=1)

    ####################################################################################################################
    # Visual analysis to pick the best minimum and maximum years.

    # Let's select those countries and windows where GDP increased more than 5%, and emissions decreased more than 5%.
    tb_count = (
        tb_change[
            (tb_change["gdp_per_capita_change"] > PCT_CHANGE_MIN)
            & (tb_change["consumption_emissions_per_capita_change"] < -PCT_CHANGE_MIN)
        ]
        .reset_index()
        .groupby(["year_min", "year_max", "n_years"], as_index=False)
        .agg({"index": "count"})
        .rename(columns={"index": "n_countries_decoupled"})
    )
    # Visually check which window has the maximum number of decoupled countries.
    import plotly.express as px

    px.line(tb_count, x="year_min", y="n_countries_decoupled", color="year_max").update_yaxes(range=[0, None])
    px.line(tb_count, x="n_years", y="n_countries_decoupled", color="year_max").update_yaxes(range=[0, None])

    # We see that the window with the maximum number of countries achieving decoupling is:
    # tb_count.sort_values("n_countries_decoupled", ascending=False).head(10)
    # The optimal windows are:
    # - 2012-2023: 47 countries.
    # - 2013-2023: 46 countries.
    # NOTE: Currently, the optimal window happens to be in the latest year.
    # If that was not the case, we'd select only the best window imposing that year_max is the latest year.

    # Given that there's not much difference, and to make the narrative simpler, we pick 2013-2023.
    year_min_best = 2013
    year_max_best = 2023

    ####################################################################################################################

    plot_decoupled_countries(
        tb_change=tb_change, year_min=year_min_best, year_max=year_max_best, pct_change_min=PCT_CHANGE_MIN
    )

    # Create a simple table with the selected window of years.
    tb_window = tb_change[
        (tb_change["year_min"] == year_min_best) & (tb_change["year_max"] == year_max_best)
    ].reset_index(drop=True)

    # Remove unnecessary columns.
    tb_window = tb_window.drop(columns=["year_min", "year_max"], errors="raise")

    # Set an appropriate index and sort conveniently.
    tb_window = tb_window.format(["country"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_window], formats=["csv"])
    ds_garden.save()
