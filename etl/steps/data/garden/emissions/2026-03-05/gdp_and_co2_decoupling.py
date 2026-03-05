"""Detect countries that have decoupled per capita GDP growth from per capita consumption-based CO2 emissions.

For each country, we apply a 5-year rolling-average window, and find the year of peak consumption-based CO2 emissions per capita (the "peak emissions year").

A country is classified as decoupled if:
1. Peak emission year is at least MIN_YEARS_SINCE_PEAK years before the latest data point.
2. Emissions in the latest year are at least PCT_CHANGE_MIN% below the level on peak emissions year.
3. GDP in the latest year is at least PCT_CHANGE_MIN% above the level on peak emissions year.

The step exports one table with GDP and emissions (original + smooth) for all years for decoupled countries, plus the peak emissions year.

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
    If RUNNING_AVERAGE_YEARS is 1, the smooth columns are just copies of the originals.
    """
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    if RUNNING_AVERAGE_YEARS > 1:
        tb["gdp_per_capita_smooth"] = tb.groupby("country", sort=False)["gdp_per_capita"].transform(
            lambda s: s.rolling(RUNNING_AVERAGE_YEARS, min_periods=1).mean()
        )
        tb["consumption_emissions_per_capita_smooth"] = tb.groupby("country", sort=False)[
            "consumption_emissions_per_capita"
        ].transform(lambda s: s.rolling(RUNNING_AVERAGE_YEARS, min_periods=1).mean())
    else:
        tb["gdp_per_capita_smooth"] = tb["gdp_per_capita"]
        tb["consumption_emissions_per_capita_smooth"] = tb["consumption_emissions_per_capita"]

    return tb


def detect_decoupled_countries(tb: Table) -> set:
    """Detect countries that have decoupled GDP growth from CO2 emissions.

    Expects tb to have a 'peak_emissions_year' column. For each country, check:
    1. The peak is at least MIN_YEARS_SINCE_PEAK years before the latest year.
    2. Smoothed emissions in the latest year are at least PCT_CHANGE_MIN% below the peak-year level.
    3. Smoothed GDP in the latest year is at least PCT_CHANGE_MIN% above the peak-year level.

    Return the set of country names that satisfy all conditions.
    """
    latest_year = int(tb["year"].max())

    decoupled = set()
    for country, tb_country in tb.groupby("country"):
        peak_year = int(tb_country["peak_emissions_year"].iloc[0])

        # Peak must be at least MIN_YEARS_SINCE_PEAK years before the latest year.
        if peak_year > latest_year - MIN_YEARS_SINCE_PEAK:
            continue

        ref_row = tb_country[tb_country["year"] == peak_year]
        latest_row = tb_country[tb_country["year"] == latest_year]
        if ref_row.empty or latest_row.empty:
            continue

        ref_gdp = float(ref_row["gdp_per_capita_smooth"].iloc[0])
        ref_emissions = float(ref_row["consumption_emissions_per_capita_smooth"].iloc[0])
        latest_gdp = float(latest_row["gdp_per_capita_smooth"].iloc[0])
        latest_emissions = float(latest_row["consumption_emissions_per_capita_smooth"].iloc[0])

        gdp_change = (latest_gdp - ref_gdp) / ref_gdp * 100
        emissions_change = (latest_emissions - ref_emissions) / ref_emissions * 100

        if emissions_change < -PCT_CHANGE_MIN and gdp_change > PCT_CHANGE_MIN:
            decoupled.add(country)

    return decoupled


def add_peak_emissions_year(tb: Table) -> Table:
    """Add a 'peak_emissions_year' column to the table.

    For each country, peak emissions year is the year of max smoothed emissions,
    ignoring partially-smoothed years (first RUNNING_AVERAGE_YEARS - 1 per country).
    """
    tb_sel = tb.copy()
    if RUNNING_AVERAGE_YEARS > 1:
        tb_sel["_year_rank"] = tb_sel.groupby("country")["year"].rank(method="first")
        tb_sel = tb_sel[tb_sel["_year_rank"] >= RUNNING_AVERAGE_YEARS].drop(columns=["_year_rank"])
    idx_peak = tb_sel.groupby("country")["consumption_emissions_per_capita_smooth"].idxmax()
    peak_years = tb_sel.loc[idx_peak, ["country", "year"]].rename(columns={"year": "peak_emissions_year"})
    tb = tb.merge(peak_years, on="country", how="left")
    return tb


def compute_changes_from_reference(tb: Table) -> Table:
    """For each country, compute % change in smoothed GDP and CO2 from its peak emissions year.

    Expects tb to have a 'peak_emissions_year' column.
    """
    results = []
    for _, tb_country in tb.groupby("country"):
        tb_country = tb_country.sort_values("year")
        ref_year = int(tb_country["peak_emissions_year"].iloc[0])
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
    show_shaded_areas: bool = False,
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

    if show_shaded_areas:
        # Shaded area for incomplete smoothing (first RUNNING_AVERAGE_YEARS - 1 years).
        min_year = int(tb_c["year"].min())
        max_year = int(tb_c["year"].max())
        last_partial_year = min_year + RUNNING_AVERAGE_YEARS - 2
        if RUNNING_AVERAGE_YEARS > 1 and last_partial_year >= min_year:
            fig.add_vrect(
                x0=min_year - 0.5,
                x1=last_partial_year + 0.5,
                fillcolor="gray",
                opacity=0.08,
                line_width=0,
                annotation_text="Incomplete smoothing",
                annotation_position="top left",
                annotation_font_size=9,
                annotation_font_color="gray",
                annotation_textangle=-90,
            )

        # Shaded area for decoupled period (from peak emissions year onward).
        if ref_year <= max_year:
            fig.add_vrect(
                x0=ref_year,
                x1=max_year + 0.5,
                fillcolor="green",
                opacity=0.05,
                line_width=0,
                annotation_text="Decoupled",
                annotation_position="top left",
                annotation_font_size=9,
                annotation_font_color="green",
                annotation_textangle=-90,
            )

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
    since_peak: bool = True,
    output_folder: Path | None = None,
) -> None:
    """Plot individual country charts as % change from a baseline year.

    Expects tb to have a 'peak_emissions_year' column.
    If since_peak=True, baseline is the peak emissions year.
    If since_peak=False, baseline is the first year in the data.
    """
    for country, tb_country in sorted(tb.groupby("country"), key=lambda x: x[0]):
        tb_country = tb_country.sort_values("year")
        ref_year = int(tb_country["peak_emissions_year"].iloc[0])

        if since_peak:
            baseline_year = ref_year
        else:
            baseline_year = int(tb_country["year"].min())

        suffix = "" if since_peak else " [full]"
        plot_country(
            tb_country=tb_country,
            country=country,
            ref_year=ref_year,
            baseline_year=baseline_year,
            title_suffix=suffix,
            output_folder=output_folder,
            show_shaded_areas=not since_peak,
        )


def plot_slope_chart_grid(
    tb: Table,
    n_cols: int = 6,
    output_file: Path | None = None,
) -> None:
    """Create a grid of slope charts with smooth curves in the background.

    Expects tb to have a 'peak_emissions_year' column.
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

    # Compute % changes from reference year for smooth curves.
    tb_changes = compute_changes_from_reference(tb=tb)
    latest_year = int(tb["year"].max())
    summary_rows = []
    for country, tb_c in tb_changes.groupby("country"):
        tb_c = tb_c.sort_values("year")
        ref_year = int(tb_c["peak_emissions_year"].iloc[0])
        last_row = tb_c.iloc[-1]
        summary_rows.append(
            {
                "country": country,
                "peak_emissions_year": ref_year,
                "latest_year": latest_year,
                "gdp_per_capita_change": float(last_row["gdp_per_capita_change"]),
                "consumption_emissions_per_capita_change": float(last_row["consumption_emissions_per_capita_change"]),
            }
        )
    tb_dec = Table(summary_rows)

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
    tb = apply_rolling_averages(tb=tb)

    # Add peak emissions year column (ignoring partially-smoothed years).
    tb = add_peak_emissions_year(tb=tb)

    # Detect decoupled countries using smoothed data.
    decoupled_countries = detect_decoupled_countries(tb=tb)

    # Uncomment to plot GDP and emissions for each individual country selected as decoupled.
    # tb_plot = tb[tb["country"].isin(decoupled_countries)]
    # plot_individual_countries(tb=tb_plot, since_peak=True, output_folder=OUTPUT_FOLDER / "countries-since-peak")
    # plot_individual_countries(tb=tb_plot, since_peak=False, output_folder=OUTPUT_FOLDER / "countries-full")
    # plot_slope_chart_grid(tb=tb_plot, output_file=OUTPUT_FOLDER / "grid.png")

    # Visual selection:
    # After visual inspection (of charts since peak emissions year, but also all years) consider removing some selected countries, for any of the following reasons:
    # - Too much variability.
    # - Upwards trend in emissions.
    # - Downwards trend in GDP.
    # decoupled_countries_dropped = {
    #     # Too much variability, possible upward trend in emissions.
    #     "Azerbaijan",
    #     # Too much variability.
    #     "Cameroon",
    #     # Upwards trend in emissions.
    #     "Croatia",
    #     # Too much variability.
    #     "Cyprus",
    #     # Possible upwards trend in emissions.
    #     "Dominican Republic",
    #     # Too much variability.
    #     "Jamaica",
    #     # Too much variability and possible upwards trend in emissions.
    #     "Kyrgyzstan",
    #     # Possible upwards trend in emissions.
    #     "Lithuania",
    #     # Upwards trend in emissions, possible downwards trend in GDP.
    #     "Nigeria",
    #     # Too much variability.
    #     "Qatar",
    #     # Too much variability and possible upwards trend in emissions.
    #     "Russia",
    #     # Too much variability and possible upwards trend in emissions.
    #     "Slovenia",
    #     # Too much variability, and upward trend in emissions.
    #     "Uruguay",
    #     # Unclear cases:
    #     # Possible downwards trend in GDP.
    #     "South Africa",
    # }
    # For now, keep all countries. We will select them visually when creating the final visualization.
    # decoupled_countries -= decoupled_countries_dropped

    # Select all years for decoupled countries.
    tb_decoupled = tb[tb["country"].isin(decoupled_countries)].reset_index(drop=True)

    # Improve table format.
    tb_decoupled = tb_decoupled.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb_decoupled])
    ds.save()
