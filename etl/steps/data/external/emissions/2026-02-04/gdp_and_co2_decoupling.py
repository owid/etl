"""This step takes the Global Carbon Budget and GDP data from World Bank's World Development Indicators, and creates a dataset with the changes in emissions and GDP over time.

We already have an interactive chart showing similar data, for per capita GDP and per capita, consumption-based CO2 emissions:
https://ourworldindata.org/grapher/co2-emissions-and-gdp

The data in the current step is not used by any grapher step, but will be used by the following static chart:

The data from this step is used in this static chart:
https://ourworldindata.org/cdn-cgi/imagedelivery/qLq-8BTgXU8yG0N6HnOy8g/f5db1a91-6bde-4430-3c09-e61fd8df9a00/w=2614

"""

from pathlib import Path

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

# Trailing running average (in years) applied to GDPpc and CO2pc levels before computing changes.
# Set to 1 to disable smoothing.
RUNNING_AVERAGE_YEARS = 3


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


def apply_rolling_averages(tb):
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    tb["gdp_per_capita"] = (
        tb["gdp_per_capita"]
        .groupby(tb["country"], sort=False)
        .rolling(RUNNING_AVERAGE_YEARS, min_periods=1)
        .mean()
        .values
    )
    tb["consumption_emissions_per_capita"] = (
        tb["consumption_emissions_per_capita"]
        .groupby(tb["country"], sort=False)
        .rolling(RUNNING_AVERAGE_YEARS, min_periods=1)
        .mean()
        .values
    )

    return tb


def decoupling_mask(tb_change: Table, pct_change_min: float) -> "pr.Series":
    return (tb_change["gdp_per_capita_change"] > pct_change_min) & (
        tb_change["consumption_emissions_per_capita_change"] < -pct_change_min
    )


def detect_decoupled_countries(
    tb_change: Table, year_min: int, year_max: int, pct_change_min: float = PCT_CHANGE_MIN
) -> set[str]:
    """Return countries that meet the decoupling criterion for a given window.

    A country is considered decoupled if GDP per capita increased by more than `PCT_CHANGE_MIN` and
    consumption-based CO2 emissions per capita decreased by more than `PCT_CHANGE_MIN` between
    `year_min` and `year_max`.
    """
    tb_sel = tb_change[
        (tb_change["year_min"] == year_min)
        & (tb_change["year_max"] == year_max)
        & decoupling_mask(tb_change=tb_change, pct_change_min=pct_change_min)
    ]
    return set(tb_sel["country"].unique())


def plot_decoupled_countries(tb_change, year_min, year_max, pct_change_min, output_folder=None):
    from pathlib import Path

    import plotly.express as px

    # Find list of countries that achieved decoupling in the selected window of years.
    countries_decoupled = sorted(
        detect_decoupled_countries(
            tb_change=tb_change, year_min=year_min, year_max=year_max, pct_change_min=pct_change_min
        )
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

    # Create output folder if saving.
    if output_folder is not None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)

    for country in countries_decoupled:
        tb_country = tb_plot_base[tb_plot_base["country"] == country].reset_index(drop=True)
        if tb_country.empty:
            continue
        # Get final percentage changes for the title.
        final_row = tb_country[tb_country["year"] == year_max]
        gdp_change = float(final_row["GDP per capita"].iloc[0])
        co2_change = float(final_row["Consumption-based CO2 per capita"].iloc[0])
        tb_plot = tb_country.melt(id_vars=["country", "year"], var_name="Indicator", value_name="value")
        fig = px.line(
            tb_plot,
            x="year",
            y="value",
            color="Indicator",
            title=f"{country} (GDP: {gdp_change:+.1f}%, CO2: {co2_change:+.1f}%)",
        ).update_yaxes(range=[-50, 50])
        if output_folder is not None:
            # Sanitize country name for filename.
            safe_name = country.replace("/", "_").replace(" ", "_")
            fig.write_image(Path(output_folder) / f"{safe_name}.png")
        else:
            fig.show()


def plot_slope_chart_grid(tb_change, year_min, year_max, pct_change_min, n_cols=6, output_file=None):
    """Create a grid of slope charts showing decoupling for each country.

    Similar to the OWID static chart style with blue GDP lines going up and red CO2 lines going down.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Find decoupled countries and sort by total decoupling magnitude (GDP increase + CO2 decrease).
    countries_decoupled = detect_decoupled_countries(
        tb_change=tb_change, year_min=year_min, year_max=year_max, pct_change_min=pct_change_min
    )
    tb_decoupled = tb_change[
        (tb_change["year_min"] == year_min)
        & (tb_change["year_max"] == year_max)
        & (tb_change["country"].isin(countries_decoupled))
    ].reset_index(drop=True)
    # To avoid spurious warnings about mixing units, remove them.
    for column in ["gdp_per_capita_change", "consumption_emissions_per_capita_change"]:
        tb_decoupled[column].metadata.unit = None
        tb_decoupled[column].metadata.short_unit = None

    tb_decoupled["decoupling_score"] = (
        tb_decoupled["gdp_per_capita_change"] - tb_decoupled["consumption_emissions_per_capita_change"]
    )
    tb_decoupled = tb_decoupled.sort_values("decoupling_score", ascending=False)

    countries = list(tb_decoupled["country"])
    n_countries = len(countries)
    n_rows = (n_countries + n_cols - 1) // n_cols

    # Calculate global y-axis range for consistent scaling across all subplots.
    y_max = tb_decoupled["gdp_per_capita_change"].max()
    y_min = tb_decoupled["consumption_emissions_per_capita_change"].min()
    # Add some padding.
    y_range = [y_min * 1.15, y_max * 1.15]

    # Create subplots grid.
    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=countries,
        vertical_spacing=0.08,
        horizontal_spacing=0.04,
    )

    # Colors matching the OWID style.
    gdp_color = "#3366cc"  # Blue
    co2_color = "#cc3333"  # Red

    for i, country in enumerate(countries):
        row = i // n_cols + 1
        col = i % n_cols + 1

        country_data = tb_decoupled[tb_decoupled["country"] == country].iloc[0]
        gdp_change = float(country_data["gdp_per_capita_change"])
        co2_change = float(country_data["consumption_emissions_per_capita_change"])

        # GDP line (0 to gdp_change).
        fig.add_trace(
            go.Scatter(
                x=[year_min, year_max],
                y=[0, gdp_change],
                mode="lines+text",
                line=dict(color=gdp_color, width=2),
                text=["", f"{gdp_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=gdp_color, size=10),
                showlegend=False,
            ),
            row=row,
            col=col,
        )

        # CO2 line (0 to co2_change).
        fig.add_trace(
            go.Scatter(
                x=[year_min, year_max],
                y=[0, co2_change],
                mode="lines+text",
                line=dict(color=co2_color, width=2),
                text=["", f"{co2_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=co2_color, size=10),
                showlegend=False,
            ),
            row=row,
            col=col,
        )

    # Update layout.
    fig.update_layout(
        height=200 * n_rows,
        width=180 * n_cols,
        title_text=f"Countries that achieved economic growth while reducing CO₂ emissions, {year_min}-{year_max}",
        showlegend=False,
    )

    # Hide axes for cleaner look, and apply consistent y-axis range.
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
    fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=True, zerolinecolor="lightgray", range=y_range)

    if output_file is not None:
        fig.write_image(output_file)
    else:
        fig.show()

    return fig


def time_window_analysis(tb_change):
    # Select those countries and windows where GDP increased more than 5%, and emissions decreased more than 5%.
    tb_count = (
        tb_change[decoupling_mask(tb_change=tb_change, pct_change_min=PCT_CHANGE_MIN)]
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
    # - 2012-2023: 45 countries.
    # - 2013-2023: 43 countries.
    # NOTE: Currently, the optimal window happens to be in the latest year.
    # But, we are only interested in changes with respecto to the latest year anyway.
    year_max_best = tb_change["year_max"].max()
    # For the lower end of the window, it could be a 20, 15, or 10 year window.
    # Even though we get more countries decoupled for 10 years, it may be too short.
    year_min_best = 2013

    # Plot the grid of slope charts of all decoupled countries.
    output_file = Path.home() / f"Downloads/smooth-grid-{year_min_best}-{year_max_best}.png"
    plot_slope_chart_grid(
        tb_change=tb_change,
        year_min=year_min_best,
        year_max=year_max_best,
        pct_change_min=PCT_CHANGE_MIN,
        output_file=output_file,
    )

    # Plot each decoupled country's full change curves individually.
    output_folder = Path.home() / f"Downloads/decoupling/smooth-countries-{year_min_best}-{year_max_best}/"
    plot_decoupled_countries(
        tb_change=tb_change,
        year_min=year_min_best,
        year_max=year_max_best,
        pct_change_min=PCT_CHANGE_MIN,
        output_folder=output_folder,
    )


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

    ####################################################################################################################
    # I noticed two abrupt peaks in Honduras emissions in 2008 and 2013 (which do not appear in territorial emissions).
    # I'll remove those years (so that Honduras is not selected if the start year is 2008 or 2013, which distorts the percentage change).
    error = "Expected two abrupt peaks in emissions in Honduras. This may have been fixed."
    _tb = tb[(tb["country"] == "Honduras")].sort_values("year")[["year", "consumption_emissions_per_capita"]].dropna()
    assert set(_tb[_tb["consumption_emissions_per_capita"].pct_change().abs() > 1]["year"]) == {2008, 2013}, error
    tb.loc[(tb["country"] == "Honduras") & (tb["year"].isin([2008, 2013])), "consumption_emissions_per_capita"] = None
    ####################################################################################################################

    # Remove rows with any missing value (we need both GDP and emissions).
    tb = tb.dropna(how="any").reset_index(drop=True)

    # Remove regions from the list of countries.
    tb = tb[~tb["country"].isin(paths.regions.regions_all)].reset_index(drop=True)

    # Smooth GDP and emissions curves (rolling average).
    if RUNNING_AVERAGE_YEARS > 1:
        tb = apply_rolling_averages(tb=tb)

    # Create a table with all possible combinations of minimum and maximum year, and the change in GDP and emissions of each country.
    tb_change = create_changes_table(tb=tb, min_window=1)

    # Analysis to pick the best minimum and maximum years.
    # time_window_analysis(tb_change=tb_change)
    year_max_best = tb_change["year_max"].max()
    year_min_best = year_max_best - 10

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
