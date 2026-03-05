"""Detect countries that have decoupled per capita GDP growth from per capita consumption-based CO2 emissions.

For each country, we apply a 5-year rolling-average window, and find the year of peak consumption-based CO2 emissions per capita (the "peak emissions year").

A country is classified as decoupled if:
1. Peak emission year is at least MIN_YEARS_SINCE_PEAK years before the latest data point.
2. Emissions in the latest year are at least PCT_CHANGE_MIN% below the level on peak emissions year.
3. GDP in the latest year is at least PCT_CHANGE_MIN% above the level on peak emissions year.

The step exports two tables:
- decoupling_full: GDP and emissions (original + smooth) for all years for decoupled countries.
- decoupling_since_peak: Same but only from peak emissions year onward, plus % change columns.

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
    # GDP per capita, PPP (constant 2021 international $).
    "ny_gdp_pcap_pp_kd": "gdp_per_capita",
    # GNI per capita, PPP (constant 2021 international $).
    # NOTE: This will be used specifically for Ireland.
    "ny_gnp_pcap_pp_kd": "gni_per_capita",
}

# Columns to select from GCB, and how to rename them.
COLUMNS_GCB = {
    "country": "country",
    "year": "year",
    "consumption_emissions_per_capita": "consumption_emissions_per_capita",
}

# Minimum percentage change (increase for GDP and decrease in CO2 emissions) for decoupling.
PCT_CHANGE_MIN = 5

# Trailing running average (in years) applied to GDPpc and CO2pc levels before computing changes.
# Set to 1 to disable smoothing.
RUNNING_AVERAGE_YEARS = 5

# Peak emissions must be at least this many years before the latest informed year.
MIN_YEARS_SINCE_PEAK = 10

# Path to local folder where charts will be saved.
# NOTE: Functions that save files will be commented by default; uncomment while doing analysis.
OUTPUT_FOLDER = Path.home() / "Documents/owid/2026-03-05_gdp_co2_decoupling_analysis/"


def fix_abrupt_changes_in_honduras(tb: Table) -> Table:
    # I noticed two abrupt peaks in Honduras emissions in 2008 and 2013 (which do not appear in territorial emissions).
    # Remove those years so they don't distort the analysis.
    error = "Expected two abrupt peaks in emissions in Honduras. This may have been fixed."
    _tb = tb[(tb["country"] == "Honduras")].sort_values("year")[["year", "consumption_emissions_per_capita"]].dropna()
    assert set(_tb[_tb["consumption_emissions_per_capita"].pct_change().abs() > 1]["year"]) == {2008, 2013}, error
    tb.loc[(tb["country"] == "Honduras") & (tb["year"].isin([2008, 2013])), "consumption_emissions_per_capita"] = None

    return tb


def apply_rolling_averages(tb: Table) -> Table:
    """Apply a trailing rolling average to GDP and emissions columns.

    New columns are added with a '_smooth' suffix, preserving the originals.
    """
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    tb["gdp_per_capita_smooth"] = tb.groupby("country", sort=False)["gdp_per_capita"].transform(
        lambda s: s.rolling(RUNNING_AVERAGE_YEARS, min_periods=1).mean()
    )
    tb["consumption_emissions_per_capita_smooth"] = tb.groupby("country", sort=False)[
        "consumption_emissions_per_capita"
    ].transform(lambda s: s.rolling(RUNNING_AVERAGE_YEARS, min_periods=1).mean())

    return tb


def detect_decoupled_countries(tb: Table) -> Table:
    """Detect countries that have decoupled GDP growth from CO2 emissions.

    For each country, we Find the year of the peak in smoothed emissions, and then:
    1. Check that this peak is at least MIN_YEARS_SINCE_PEAK years before the latest year.
    2. Check if smoothed emissions in the latest year are at least PCT_CHANGE_MIN% below the peak-year level.
    3. Check if smoothed GDP in the latest year is at least PCT_CHANGE_MIN% above the peak-year level.

    Return a table with columns: country, peak_emissions_year, latest_year, gdp_per_capita_change,
    consumption_emissions_per_capita_change.
    """
    # Find the latest year per country.
    latest_years = tb.groupby("country", as_index=False)["year"].max().rename(columns={"year": "latest_year"})
    error = "Expected latest year informed for all countries to be the same."
    assert set(latest_years["latest_year"]) == set([tb["year"].max()]), error

    # Find the peak in smoothed emissions for each country (across all years).
    idx_peak = tb.groupby("country")["consumption_emissions_per_capita_smooth"].idxmax()
    tb_ref = tb.loc[
        idx_peak, ["country", "year", "gdp_per_capita_smooth", "consumption_emissions_per_capita_smooth"]
    ].rename(
        columns={
            "year": "peak_emissions_year",
            "gdp_per_capita_smooth": "ref_gdp",
            "consumption_emissions_per_capita_smooth": "ref_emissions",
        }
    )

    # Merge with latest years and filter: peak must be at least MIN_YEARS_SINCE_PEAK before the latest year.
    tb_ref = tb_ref.merge(latest_years, on="country", how="left")
    tb_ref = tb_ref[tb_ref["peak_emissions_year"] <= (tb_ref["latest_year"] - MIN_YEARS_SINCE_PEAK)].reset_index(
        drop=True
    )

    # Drop unnecessary columns.
    tb_ref = tb_ref.drop(columns=["latest_year"])

    # Find the latest year values per country.
    idx_latest = tb.groupby("country")["year"].idxmax()
    tb_latest = tb.loc[
        idx_latest, ["country", "year", "gdp_per_capita_smooth", "consumption_emissions_per_capita_smooth"]
    ].rename(
        columns={
            "year": "latest_year",
            "gdp_per_capita_smooth": "latest_gdp",
            "consumption_emissions_per_capita_smooth": "latest_emissions",
        }
    )

    # Merge reference and latest values.
    tb_merged = tb_ref.merge(tb_latest, on="country", how="inner")

    # Compute percentage changes.
    tb_merged["gdp_per_capita_change"] = (tb_merged["latest_gdp"] - tb_merged["ref_gdp"]) / tb_merged["ref_gdp"] * 100
    tb_merged["consumption_emissions_per_capita_change"] = (
        (tb_merged["latest_emissions"] - tb_merged["ref_emissions"]) / tb_merged["ref_emissions"] * 100
    )

    # Apply decoupling conditions.
    tb_result = tb_merged[
        (tb_merged["consumption_emissions_per_capita_change"] < -PCT_CHANGE_MIN)
        & (tb_merged["gdp_per_capita_change"] > PCT_CHANGE_MIN)
    ][
        [
            "country",
            "peak_emissions_year",
            "latest_year",
            "gdp_per_capita_change",
            "consumption_emissions_per_capita_change",
        ]
    ].reset_index(drop=True)

    return tb_result


def compute_changes_from_reference(tb: Table, tb_decoupled: Table) -> Table:
    """For each decoupled country, compute % change in smoothed GDP and CO2 from its reference year."""
    results = []
    for _, row in tb_decoupled.iterrows():
        country = row["country"]
        ref_year = int(row["peak_emissions_year"])

        tb_country = tb[tb["country"] == country].sort_values("year")
        ref_row = tb_country[tb_country["year"] == ref_year].iloc[0]
        ref_gdp = float(ref_row["gdp_per_capita_smooth"])
        ref_emissions = float(ref_row["consumption_emissions_per_capita_smooth"])

        tb_after = tb_country[tb_country["year"] >= ref_year].copy()
        tb_after["gdp_per_capita_change"] = (tb_after["gdp_per_capita_smooth"] - ref_gdp) / ref_gdp * 100
        tb_after["consumption_emissions_per_capita_change"] = (
            (tb_after["consumption_emissions_per_capita_smooth"] - ref_emissions) / ref_emissions * 100
        )
        tb_after["peak_emissions_year"] = ref_year
        results.append(tb_after)

    return pr.concat(results, ignore_index=True)


def plot_country(
    tb_country: Table,
    country: str,
    ref_year: int,
    baseline_year: int,
    title_suffix: str = "",
    output_folder: Path | None = None,
) -> None:
    """Plot a single country's GDP and emissions as % change from baseline year (original + smooth).

    A vertical dashed line marks the peak emissions year.
    """
    import plotly.graph_objects as go

    gdp_color = "#3366cc"
    co2_color = "#cc3333"
    gdp_color_light = "rgba(51, 102, 204, 0.25)"
    co2_color_light = "rgba(204, 51, 51, 0.25)"

    # Get baseline values (at baseline_year).
    baseline_row = tb_country[tb_country["year"] == baseline_year]
    if baseline_row.empty:
        return
    base_gdp = float(baseline_row["gdp_per_capita"].iloc[0])
    base_co2 = float(baseline_row["consumption_emissions_per_capita"].iloc[0])
    base_gdp_smooth = float(baseline_row["gdp_per_capita_smooth"].iloc[0])
    base_co2_smooth = float(baseline_row["consumption_emissions_per_capita_smooth"].iloc[0])

    # Filter to years >= baseline_year.
    tb_c = tb_country[tb_country["year"] >= baseline_year].copy()

    # Compute % change from baseline.
    gdp_pct = (tb_c["gdp_per_capita"] - base_gdp) / base_gdp * 100
    co2_pct = (tb_c["consumption_emissions_per_capita"] - base_co2) / base_co2 * 100
    gdp_smooth_pct = (tb_c["gdp_per_capita_smooth"] - base_gdp_smooth) / base_gdp_smooth * 100
    co2_smooth_pct = (tb_c["consumption_emissions_per_capita_smooth"] - base_co2_smooth) / base_co2_smooth * 100

    fig = go.Figure()

    # Original curves (background, lighter).
    fig.add_trace(
        go.Scatter(
            x=tb_c["year"], y=gdp_pct, mode="lines", line=dict(color=gdp_color_light, width=1), name="GDP (original)"
        )
    )
    fig.add_trace(
        go.Scatter(
            x=tb_c["year"], y=co2_pct, mode="lines", line=dict(color=co2_color_light, width=1), name="CO2 (original)"
        )
    )

    # Smooth curves (foreground, solid).
    fig.add_trace(
        go.Scatter(
            x=tb_c["year"], y=gdp_smooth_pct, mode="lines", line=dict(color=gdp_color, width=2), name="GDP (smooth)"
        )
    )
    fig.add_trace(
        go.Scatter(
            x=tb_c["year"], y=co2_smooth_pct, mode="lines", line=dict(color=co2_color, width=2), name="CO2 (smooth)"
        )
    )

    # Vertical line at peak emissions year.
    fig.add_vline(x=ref_year, line_dash="dash", line_color="gray", opacity=0.5)

    # Final % changes for the title.
    final_gdp = float(gdp_smooth_pct.iloc[-1])
    final_co2 = float(co2_smooth_pct.iloc[-1])

    fig.update_layout(
        title=f"{country} (peak: {ref_year}, GDP: {final_gdp:+.1f}%, CO2: {final_co2:+.1f}%){title_suffix}",
        xaxis_title="Year",
        yaxis_title="% change",
    )

    if output_folder is not None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        safe_name = country.replace("/", "_").replace(" ", "_")
        fig.write_image(Path(output_folder) / f"{safe_name}.png")
    else:
        fig.show()


def plot_individual_countries(
    tb: Table,
    tb_decoupled: Table,
    since_peak: bool = True,
    output_folder: Path | None = None,
) -> None:
    """Plot individual country charts as % change from a baseline year.

    If since_peak=True, baseline is the peak emissions year.
    If since_peak=False, baseline is the first fully-informed rolling average year (min_year + window - 1).
    """
    for _, row in tb_decoupled.iterrows():
        country = row["country"]
        ref_year = int(row["peak_emissions_year"])

        tb_country = tb[tb["country"] == country].sort_values("year")

        if since_peak:
            baseline_year = ref_year
        else:
            # First year with a fully-informed rolling average.
            baseline_year = int(tb_country["year"].min()) + RUNNING_AVERAGE_YEARS - 1

        suffix = "" if since_peak else " [full]"
        plot_country(
            tb_country=tb_country,
            country=country,
            ref_year=ref_year,
            baseline_year=baseline_year,
            title_suffix=suffix,
            output_folder=output_folder,
        )


def plot_slope_chart_grid(
    tb: Table,
    tb_decoupled: Table,
    n_cols: int = 6,
    output_file: Path | None = None,
) -> None:
    """Create a grid of slope charts with smooth curves in the background.

    Each subplot shows:
    - Straight slope lines from 0% (at reference year) to final % change (at latest year).
    - Smooth year-by-year % change curves in the background (softer colors).

    All subplots share the same x-axis range. Countries are sorted by:
    1. Years since peak (desc)
    2. GDP change - emissions change (desc)
    3. GDP change (desc)
    4. Emissions change (asc)
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Ensure output folder exists.
    if not output_file.parent.exists():
        Path(output_file.parent).mkdir(parents=True, exist_ok=True)

    tb_dec = tb_decoupled.copy()

    # Sort countries.
    tb_dec["years_since_peak"] = tb_dec["latest_year"] - tb_dec["peak_emissions_year"]
    tb_dec["decoupling_score"] = (
        tb_dec["gdp_per_capita_change"] - tb_dec["consumption_emissions_per_capita_change"].values
    )
    tb_dec = tb_dec.sort_values(
        ["years_since_peak", "decoupling_score", "gdp_per_capita_change", "consumption_emissions_per_capita_change"],
        ascending=[False, False, False, True],
    )

    countries = list(tb_dec["country"])
    n_countries = len(countries)
    n_rows = (n_countries + n_cols - 1) // n_cols

    # Compute % changes from reference year for smooth curves.
    tb_changes = compute_changes_from_reference(tb=tb, tb_decoupled=tb_dec)

    # Global x-axis range: from earliest reference year to latest year.
    x_min = int(tb_dec["peak_emissions_year"].min())
    x_max = int(tb_dec["latest_year"].max())

    # Global y-axis range (consider both slope endpoints and smooth curves).
    y_max_val = max(float(tb_dec["gdp_per_capita_change"].max()), float(tb_changes["gdp_per_capita_change"].max()))
    y_min_val = min(
        float(tb_dec["consumption_emissions_per_capita_change"].min()),
        float(tb_changes["consumption_emissions_per_capita_change"].min()),
    )
    y_range = [y_min_val * 1.15, y_max_val * 1.15]

    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=countries,
        vertical_spacing=0.08,
        horizontal_spacing=0.04,
    )

    gdp_color = "#3366cc"
    co2_color = "#cc3333"
    gdp_color_light = "rgba(51, 102, 204, 0.3)"
    co2_color_light = "rgba(204, 51, 51, 0.3)"

    for i, country in enumerate(countries):
        row = i // n_cols + 1
        col = i % n_cols + 1

        country_data = tb_dec[tb_dec["country"] == country].iloc[0]
        ref_year = int(country_data["peak_emissions_year"])
        latest_year = int(country_data["latest_year"])
        gdp_change = float(country_data["gdp_per_capita_change"])
        co2_change = float(country_data["consumption_emissions_per_capita_change"])

        # Smooth curves in background.
        tb_c = tb_changes[tb_changes["country"] == country].sort_values("year")
        fig.add_trace(
            go.Scatter(
                x=tb_c["year"],
                y=tb_c["gdp_per_capita_change"],
                mode="lines",
                line=dict(color=gdp_color_light, width=1),
                showlegend=False,
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=tb_c["year"],
                y=tb_c["consumption_emissions_per_capita_change"],
                mode="lines",
                line=dict(color=co2_color_light, width=1),
                showlegend=False,
            ),
            row=row,
            col=col,
        )

        # Slope lines (foreground).
        fig.add_trace(
            go.Scatter(
                x=[ref_year, latest_year],
                y=[0, gdp_change],
                mode="lines+text",
                line=dict(color=gdp_color, width=2),
                text=["", f"{gdp_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=gdp_color, size=10),
                showlegend=False,
                cliponaxis=False,
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=[ref_year, latest_year],
                y=[0, co2_change],
                mode="lines+text",
                line=dict(color=co2_color, width=2),
                text=["", f"{co2_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=co2_color, size=10),
                showlegend=False,
                cliponaxis=False,
            ),
            row=row,
            col=col,
        )

    fig.update_layout(
        height=200 * n_rows,
        width=180 * n_cols,
        title_text=f"Countries that achieved economic growth while reducing CO₂ emissions (peak year → {x_max})",
        showlegend=False,
    )

    # Shared x-axis range across all subplots.
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False, range=[x_min - 1, x_max + 1])
    fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=True, zerolinecolor="lightgray", range=y_range)

    if output_file is not None:
        fig.write_image(output_file)
    else:
        fig.show()


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

    # Use GNI instead of GDP for Ireland.
    # NOTE: For simplicity, we keep calling it GDP, but the chart will have this clarification.
    tb_wdi.loc[tb_wdi["country"] == "Ireland", "gdp_per_capita"] = tb_wdi.loc[
        tb_wdi["country"] == "Ireland", "gni_per_capita"
    ]
    tb_wdi = tb_wdi.drop(columns=["gni_per_capita"], errors="raise")

    # Combine both tables.
    tb = tb_gcb.merge(tb_wdi, on=["country", "year"], how="inner", short_name=paths.short_name)

    # Fix abrupt issues with Honduras emissions data.
    tb = fix_abrupt_changes_in_honduras(tb=tb)

    # Remove rows with any missing value (we need both GDP and emissions).
    tb = tb.dropna(how="any").reset_index(drop=True)

    # Remove regions from the list of countries.
    tb = tb[~tb["country"].isin(paths.regions.regions_all)].reset_index(drop=True)

    # Apply rolling averages (adds _smooth columns, keeps originals).
    if RUNNING_AVERAGE_YEARS > 1:
        tb = apply_rolling_averages(tb=tb)
    else:
        tb["gdp_per_capita_smooth"] = tb["gdp_per_capita"]
        tb["consumption_emissions_per_capita_smooth"] = tb["consumption_emissions_per_capita"]

    # Detect decoupled countries using smoothed data.
    tb_decoupled = detect_decoupled_countries(tb=tb)

    # Uncomment to plot GDP and emissions for each individual country selected as decoupled.
    # NOTE: It's convenient to visualize the curves since peak emissions year, but also the complete trend.
    # plot_individual_countries(tb=tb, tb_decoupled=tb_decoupled, since_peak=True, output_folder=OUTPUT_FOLDER / "countries-since-peak")
    # plot_individual_countries(tb=tb, tb_decoupled=tb_decoupled, since_peak=False, output_folder=OUTPUT_FOLDER / "countries-full")
    # plot_slope_chart_grid(tb=tb, tb_decoupled=tb_decoupled, output_file=OUTPUT_FOLDER / "grid.png")

    # Visual selection:
    # After visual inspection (of charts since peak emissions year, but also all years) we remove countries, for any of the following reasons:
    # - Too much variability.
    # - Upwards trend in emissions.
    # - Downwards trend in GDP.
    decoupled_countries_dropped = {
        # Too much variability, possible upward trend in emissions.
        "Azerbaijan",
        # Too much variability.
        "Cameroon",
        # Upwards trend in emissions.
        "Croatia",
        # Too much variability.
        "Cyprus",
        # Possible upwards trend in emissions.
        "Dominican Republic",
        # Too much variability.
        "Jamaica",
        # Too much variability.
        "Jordan",
        # Too much variability and possible upwards trend in emissions.
        "Kazakhstan",
        # Too much variability and possible upwards trend in emissions.
        "Kyrgyzstan",
        # Possible upwards trend in emissions.
        "Lithuania",
        # Upwards trend in emissions, possible downwards trend in GDP.
        "Nigeria",
        # Too much variability.
        "Qatar",
        # Too much variability and possible upwards trend in emissions.
        "Russia",
        # Too much variability and possible upwards trend in emissions.
        "Slovenia",
        # Too much variability, and upward trend in emissions.
        "Uruguay",
        # Unclear cases:
        # Possible downwards trend in GDP.
        "South Africa",
    }

    # Update the table of decoupled countries.
    tb_decoupled = tb_decoupled[~tb_decoupled["country"].isin(decoupled_countries_dropped)].reset_index(drop=True)

    # Uncomment to plot the grid of selected decoupled countries.
    # plot_slope_chart_grid(tb=tb, tb_decoupled=tb_decoupled, output_file=OUTPUT_FOLDER / "grid_selected.png")
    # plot_individual_countries(tb=tb, tb_decoupled=tb_decoupled, since_peak=True, output_folder=OUTPUT_FOLDER / "decoupled-countries-since-peak")

    # Prepare table with data since peak year for only decoupled countries, with % change columns, for all years.
    tb_all_years = compute_changes_from_reference(tb=tb, tb_decoupled=tb_decoupled)
    tb_all_years = tb_all_years[
        [
            "country",
            "year",
            "gdp_per_capita",
            "consumption_emissions_per_capita",
            "gdp_per_capita_smooth",
            "consumption_emissions_per_capita_smooth",
            "gdp_per_capita_change",
            "consumption_emissions_per_capita_change",
        ]
    ].reset_index(drop=True)
    tb_all_years = tb_all_years.format(["country", "year"], short_name=paths.short_name)

    # Prepare another table with only the first (peak emissions year) and last year for each country.
    tb_first_and_last_year = pr.concat(
        [
            tb_all_years.reset_index().groupby("country", as_index=False).first(),
            tb_all_years.reset_index().groupby("country", as_index=False).last(),
        ],
        ignore_index=True,
    )
    tb_first_and_last_year = tb_first_and_last_year.format(
        ["country", "year"], short_name=paths.short_name + "_first_and_last_year"
    )

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb_all_years, tb_first_and_last_year], formats=["feather", "csv"])
    ds.save()
