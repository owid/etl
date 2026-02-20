"""Create a dataset with indicators on food supply (in kilocalories) and agricultural land use (in hectares).

TODO: Check if there's an update on the relevant indicators in FAOSTAT.

The goal is to create a visualization showing that some countries managed to feed more people with less land.
In other words, we need to select countries that have managed to decouple per capita food supply and land use.

The main issue is that food supply includes imported food that was produced in land of a different country.
So we need to find a way to avoid showing decoupled countries that simply offshored their land use.

We propose the following approach:
- We use 3-year rollowing averages, to avoid variability at the year level (e.g. due to COVID, bad harvest, stock changes, etc.).
# TODO: Should we consider a longer rolling window? Those three years may be particularly low due to COVID.
- We pick our relevant window, which will be 1963 (meaning the average of 1961, 1962, and 1963) to 2022 (meaning the average of 2020, 2021, 2022). This is the biggest window possible given the available data.
# TODO: Should we consider a shorter baseline window (following a similar logic to the step on decoupling GDP and emissions)?
- We select countries where:
  - Food supply has increased in that period by at least 5%.
  - Land use has decreased in that period by at least 5%.
  - Imports are lower than 20% of domestic supply in the latest year. This imports cap avoids selecting countries that have offshored land use, i.e. they are feeding more people by using more land elsewhere.

TODO: Analogously, we could consider applying a cap on exports in the first year. It's possible that some countries used to export a lot of food and now they export very little. Those countries may show an increase in food supply and a significant decrease in land use; but this decrease in land use would be due to feeding fewer people elsewhere, rather than feeding more people in their own country. I'm not sure if this is necessary, and I imagine this may not be a common case. Try it and see if it makes a relevant difference.

"""

from pathlib import Path

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum percentage increase in food supply in the latest year with respect to the first year.
FOOD_INCREASE_PCT_MIN = 5
# Minimum percentage decrease in land use in the latest year with respect to the first year.
LAND_DECREASE_PCT_MIN = 5
# Maximum imports as a percentage of domestic supply (applied to the latest year only).
# TODO: Implement.
IMPORTS_SHARE_OF_SUPPLY_MAX = 20

# Number of years for the rolling average (1 to not do any rolling average).
ROLLING_AVERAGE_YEARS = 3

# Path to local folder where charts will be saved.
# NOTE: Functions that save files will be commented by default; uncomment while doing analysis.
OUTPUT_FOLDER = Path.home() / "Documents/owid/2026-02-20_food_decoupling_analysis/"


def create_changes_table(tb, columns, min_window=1):
    """Create a table with percent changes in various columns, for all country-window combinations."""
    years = sorted(tb["year"].unique())
    max_window = int(tb["year"].max() - tb["year"].min())

    results = []
    for window_size in range(min_window, max_window + 1):
        for year_min in years:
            year_max = year_min + window_size
            if year_max not in years:
                continue

            tb_start = tb[tb["year"] == year_min][["country"] + columns]
            tb_end = tb[tb["year"] == year_max][["country"] + columns]

            tb_merged = tb_start.merge(tb_end, on="country", suffixes=("_start", "_end"))

            tb_merged["year_min"] = year_min
            tb_merged["year_max"] = year_max
            tb_merged["n_years"] = year_max - year_min
            for column in columns:
                tb_merged[f"{column}_change"] = (
                    (tb_merged[f"{column}_end"] - tb_merged[f"{column}_start"]) / tb_merged[f"{column}_start"] * 100
                )
            results.append(
                tb_merged[["country", "year_min", "year_max", "n_years"] + [f"{column}_change" for column in columns]]
            )

    return pr.concat(results, ignore_index=True)


def decoupling_mask(tb_change):
    return (tb_change["food_energy_change"] > FOOD_INCREASE_PCT_MIN) & (
        tb_change["agricultural_land_change"] < -LAND_DECREASE_PCT_MIN
    )


def detect_decoupled_countries(tb_change, year_min, year_max):
    """Return countries that meet the decoupling criterion for a given window.

    A country is considered decoupled if GDP per capita increased by more than `PCT_CHANGE_MIN` and
    consumption-based CO2 emissions per capita decreased by more than `PCT_CHANGE_MIN` between
    `year_min` and `year_max`.
    """
    tb_sel = tb_change[
        (tb_change["year_min"] == year_min) & (tb_change["year_max"] == year_max) & decoupling_mask(tb_change=tb_change)
    ]
    return set(tb_sel["country"].unique())


def plot_decoupled_countries(tb_change, countries, year_min, year_max, y_min=-50, y_max=50, output_folder=None):
    import plotly.express as px

    countries_decoupled = sorted(countries)

    # Filter tb_change for the time series: fixed year_min, varying year_max from year_min to year_max.
    tb_plot_base = tb_change[
        (tb_change["country"].isin(countries_decoupled))
        & (tb_change["year_min"] == year_min)
        & (tb_change["year_max"] <= year_max)
    ][["country", "year_max", "food_energy_change", "agricultural_land_change"]].copy()

    tb_plot_base = tb_plot_base.rename(
        columns={
            "year_max": "year",
            "food_energy_change": "Food supply per capita",
            "agricultural_land_change": "Agricultural land",
        }
    )

    # Add baseline point at year_min with 0% change.
    from owid.catalog import Table

    tb_plot_base = pr.concat(
        [
            Table(
                {
                    "country": countries_decoupled,
                    "year": year_min,
                    "Food supply per capita": 0,
                    "Agricultural land": 0,
                }
            ),
            tb_plot_base,
        ],
        ignore_index=True,
    )

    # To avoid spurious warnings about mixing units, remove them.
    for column in ["Food supply per capita", "Agricultural land"]:
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
        if final_row.empty:
            # Country doesn't have data for the full window; skip it.
            print(f"Not enough data for this window of years for {country}")
            continue
        food_change = float(final_row["Food supply per capita"].iloc[0])
        land_change = float(final_row["Agricultural land"].iloc[0])
        tb_plot = tb_country.melt(id_vars=["country", "year"], var_name="Indicator", value_name="value")
        fig = px.line(
            tb_plot,
            x="year",
            y="value",
            color="Indicator",
            title=f"{country} (Food supply: {food_change:+.1f}%, Land use: {land_change:+.1f}%)",
        ).update_yaxes(range=[y_min, y_max])
        if output_folder is not None:
            # Sanitize country name for filename.
            safe_name = country.replace("/", "_").replace(" ", "_")
            fig.write_image(Path(output_folder) / f"{safe_name}.png")
        else:
            fig.show()


def plot_slope_chart_grid(tb_change, countries_decoupled, year_min, year_max, n_cols=6, output_file=None):
    """Create a grid of slope charts showing decoupling for each country."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    tb_decoupled = tb_change[
        (tb_change["year_min"] == year_min)
        & (tb_change["year_max"] == year_max)
        & (tb_change["country"].isin(countries_decoupled))
    ].reset_index(drop=True)
    # To avoid spurious warnings about mixing units, remove them.
    for column in ["food_energy_change", "agricultural_land_change"]:
        tb_decoupled[column].metadata.unit = None
        tb_decoupled[column].metadata.short_unit = None

    tb_decoupled["decoupling_score"] = tb_decoupled["food_energy_change"] - tb_decoupled["agricultural_land_change"]
    tb_decoupled = tb_decoupled.sort_values("decoupling_score", ascending=False)

    countries_decoupled = list(tb_decoupled["country"])
    n_countries = len(countries_decoupled)
    n_rows = (n_countries + n_cols - 1) // n_cols

    # Calculate global y-axis range for consistent scaling across all subplots.
    y_max = tb_decoupled["food_energy_change"].max()
    y_min = tb_decoupled["agricultural_land_change"].min()
    # Add some padding.
    y_range = [y_min * 1.15, y_max * 1.15]

    # Create subplots grid.
    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=countries_decoupled,
        vertical_spacing=0.08,
        horizontal_spacing=0.04,
    )

    # Define colors (blue for food and red for land).
    food_color = "#3366cc"
    land_color = "#cc3333"

    for i, country in enumerate(countries_decoupled):
        row = i // n_cols + 1
        col = i % n_cols + 1

        country_data = tb_decoupled[tb_decoupled["country"] == country].iloc[0]
        food_change = float(country_data["food_energy_change"])
        land_change = float(country_data["agricultural_land_change"])

        # Food supply line.
        fig.add_trace(
            go.Scatter(
                x=[year_min, year_max],
                y=[0, food_change],
                mode="lines+text",
                line=dict(color=food_color, width=2),
                text=["", f"{food_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=food_color, size=10),
                showlegend=False,
                cliponaxis=False,
            ),
            row=row,
            col=col,
        )

        # Land use line.
        fig.add_trace(
            go.Scatter(
                x=[year_min, year_max],
                y=[0, land_change],
                mode="lines+text",
                line=dict(color=land_color, width=2),
                text=["", f"{land_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=land_color, size=10),
                showlegend=False,
                cliponaxis=False,
            ),
            row=row,
            col=col,
        )

    # Update layout.
    fig.update_layout(
        height=200 * n_rows,
        width=180 * n_cols,
        title_text=f"Countries that achieved food supply increase while reducing land use, {year_min}-{year_max}",
        showlegend=False,
    )

    # Hide axes for cleaner look, and apply consistent y-axis range.
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
    fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=True, zerolinecolor="lightgray", range=y_range)

    if output_file is not None:
        fig.write_image(output_file)
    else:
        fig.show()


def time_window_analysis(tb_change) -> None:
    import plotly.express as px

    # Select those countries and windows where GDP increased more than 5%, and emissions decreased more than 5%.
    tb_count = (
        tb_change[decoupling_mask(tb_change=tb_change)]
        .reset_index()
        .groupby(["year_min", "year_max", "n_years"], as_index=False)
        .agg({"index": "count"})
        .rename(columns={"index": "n_countries_decoupled"})
    )
    # Visually check which window has the maximum number of decoupled countries.

    # Plot the number of decoupled countries for each choice of year max, as a function of year min.
    output_file = OUTPUT_FOLDER / "n-decoupled-countries-vs-year-min.png"
    # Ensure folder exists.
    OUTPUT_FOLDER.mkdir(exist_ok=True)
    color_map = {y: "blue" if y == tb_count["year_max"].max() else "lightgray" for y in tb_count["year_max"].unique()}
    fig = px.line(
        tb_count.sort_values(["year_max", "year_min"], ascending=False),
        x="year_min",
        y="n_countries_decoupled",
        color="year_max",
        color_discrete_map=color_map,
    ).update_yaxes(range=[0, None])
    fig.write_image(output_file)

    # Plot the number of decoupled countries for each choice of year max, as a function of the window size.
    output_file = OUTPUT_FOLDER / "n-decoupled-countries-vs-n-years.png"
    fig = px.line(
        tb_count.sort_values(["year_max", "year_min"], ascending=False),
        x="n_years",
        y="n_countries_decoupled",
        color="year_max",
        color_discrete_map=color_map,
    ).update_yaxes(range=[0, None])
    fig.write_image(output_file)

    # We see that the window with the maximum number of countries achieving decoupling is:
    tb_count.sort_values("n_countries_decoupled", ascending=False).head(10)

    # The full window of years is the maximum year minus the minium; but if we use a rolling average, the minimum year should be shifted forward to the end of the window.
    years_full_window = tb_change["year_max"].max() - (tb_change["year_min"].min() + max(1, ROLLING_AVERAGE_YEARS) - 1)
    # Then the choices are:
    tb_count[
        (tb_count["year_max"] == tb_count["year_max"].max()) & (tb_count["n_years"].isin([10, 15, 20]))
    ].sort_values("n_countries_decoupled", ascending=False)
    year_max_best = tb_change["year_max"].max()
    # For the lower end of the window, it could be a 20, 15, or 10 year window.
    # for window in [years_full_window, 50, 40, 30, 20, 10]:
    for window in [years_full_window]:
        year_min_best = year_max_best - window

        # Selected list of countries decoupled.
        countries_decoupled = detect_decoupled_countries(
            tb_change=tb_change, year_min=year_min_best, year_max=year_max_best
        )

        print(
            f"\n- Window of {window} years ({year_min_best}-{year_max_best}): {len(countries_decoupled)} countries selected. Of those:"
        )
        mask_selected = (
            (tb_change["country"].isin(countries_decoupled))
            & (tb_change["year_min"] == year_min_best)
            & (tb_change["year_max"] <= year_max_best)
        )
        _lower_land = (
            tb_change[mask_selected]
            .groupby("country", as_index=False)
            .agg({"agricultural_land_change": lambda x: (x < 0).all()})
        )
        lower_land = set(_lower_land[_lower_land["agricultural_land_change"]]["country"])
        _higher_food = (
            tb_change[mask_selected]
            .groupby("country", as_index=False)
            .agg({"food_energy_change": lambda x: (x > 0).all()})
        )
        higher_food = set(_higher_food[_higher_food["food_energy_change"]]["country"])
        lower_land_and_higher_food = lower_land & higher_food
        print(f"    - {len(lower_land)} consistently stayed below the land use levels of {year_min_best}.")
        print(f"    - {len(higher_food)} consistently stayed above the food supply levels of {year_min_best}.")
        print(f"    - {len(lower_land_and_higher_food)} consistently achieved both.")

        # Plot the grid of slope charts of all decoupled countries.
        output_file = OUTPUT_FOLDER / f"smooth-grid-{year_min_best}-{year_max_best}.png"
        plot_slope_chart_grid(
            tb_change=tb_change,
            countries_decoupled=countries_decoupled,
            year_min=year_min_best,
            year_max=year_max_best,
            output_file=output_file,
        )

        # Plot each decoupled country's change curves individually, within the selected window.
        output_folder = OUTPUT_FOLDER / f"smooth-countries-{year_min_best}-{year_max_best}/"
        plot_decoupled_countries(
            tb_change=tb_change,
            countries=countries_decoupled,
            year_min=year_min_best,
            year_max=year_max_best,
            output_folder=output_folder,
        )


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT combined food balances dataset, and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # Load FAOSTAT land use dataset, and read its main table.
    ds_rl = paths.load_dataset("faostat_rl")
    tb_rl = ds_rl.read("faostat_rl")

    #
    # Process data.
    #
    # Select relevant elements from the land use data.
    # Item "Agricultural land" (00006610).
    # Element code "Area" (005110) in hectares.
    tb_rl = tb_rl[(tb_rl["element_code"] == "005110") & (tb_rl["item_code"] == "00006610")].reset_index(drop=True)
    error = "Units of area have changed."
    assert set(tb_rl["unit"]) == {"hectares"}, error
    tb_rl = tb_rl[["country", "year", "value"]].rename(columns={"value": "agricultural_land"}, errors="raise")

    # Select relevant elements from the food balances data.
    # Select item "Total" ("00002901").
    # Select element "Food available for consumption" ("0664pc"), in kcal/capita/day. It was converted from the original "Food supply (kcal/capita/day)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    tb = tb_fbsc[(tb_fbsc["element_code"] == "0664pc") & (tb_fbsc["item_code"] == "00002901")].reset_index(drop=True)
    # Sanity check.
    assert set(tb["element"]) == {"Food available for consumption"}
    assert set(tb["unit"]) == {"kilocalories per day per capita"}
    assert tb[tb["value"].isnull()].empty
    # Keep only relevant columns.
    tb = tb[["country", "year", "value"]].rename(columns={"value": "food_energy"}, errors="raise")

    ####################################################################################################################
    # TODO: Consider removing:
    # I think the most meaningful metrics to use are:
    # - Per capita food supply (in kcal/capita/day).
    # - Land use (in ha/year).
    # Alternatively, we could use per capita yearly food supply (kcal/capita/year), or total yearly food supply (kcal/year).
    # Convert kcal/capita/day to kcal/capita/year.
    # tb.loc[(tb["element_code"] == "0664pc"), "food_energy"] *= 365

    # Calculate the total food supply (not per capita).
    # tb_total = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    # tb_total["food_energy"] *= tb_total["population"]
    ####################################################################################################################

    # Combine food supply and agricultural land.
    tb = tb.merge(tb_rl, on=["country", "year"], how="outer")

    # Remove regions from the list of countries.
    tb = tb[~tb["country"].isin(paths.regions.regions_all)].reset_index(drop=True)
    # Remove custom (FAO) regions.
    tb = tb[~tb["country"].str.contains("(FAO)", regex=False)].reset_index(drop=True)

    # Keep only rows that have data on both food supply and land use.
    _n_rows_before = len(tb)
    tb = tb.dropna(how="any").reset_index(drop=True)
    error = "Unexpectedly high number of rows lost when merging food supply and land use."
    assert (100 * (_n_rows_before - len(tb)) / _n_rows_before) < 23, error

    # Replace series by a rolling average.
    if ROLLING_AVERAGE_YEARS > 1:
        tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
        for column in ["food_energy", "agricultural_land"]:
            tb[column] = tb.groupby("country", sort=False)[column].transform(
                lambda s: s.rolling(ROLLING_AVERAGE_YEARS, min_periods=1).mean()
            )

    # Create a table with all possible combinations of minimum and maximum year, and the change in food supply and land use of each country.
    tb_change = create_changes_table(tb, columns=["food_energy", "agricultural_land"])

    # Analysis to pick the best minimum and maximum years.
    # time_window_analysis(tb_change=tb_change)

    # Results for different windows:
    # - Window of 59 years (1963-2022): 43 countries selected. Of those:
    #     - 15 consistently stayed below the land use levels of 1963.
    #     - 23 consistently stayed above the food supply levels of 1963.
    #     - 7 consistently achieved both.
    # - Window of 50 years (1972-2022): 40 countries selected. Of those:
    #     - 15 consistently stayed below the land use levels of 1972.
    #     - 16 consistently stayed above the food supply levels of 1972.
    #     - 3 consistently achieved both.
    # - Window of 40 years (1982-2022): 41 countries selected. Of those:
    #     - 17 consistently stayed below the land use levels of 1982.
    #     - 7 consistently stayed above the food supply levels of 1982.
    #     - 1 consistently achieved both.
    # - Window of 30 years (1992-2022): 42 countries selected. Of those:
    #     - 19 consistently stayed below the land use levels of 1992.
    #     - 16 consistently stayed above the food supply levels of 1992.
    #     - 7 consistently achieved both.
    # - Window of 20 years (2002-2022): 31 countries selected. Of those:
    #     - 21 consistently stayed below the land use levels of 2002.
    #     - 9 consistently stayed above the food supply levels of 2002.
    #     - 7 consistently achieved both.
    # - Window of 10 years (2012-2022): 9 countries selected. Of those:
    #     - 9 consistently stayed below the land use levels of 2012.
    #     - 6 consistently stayed above the food supply levels of 2012.
    #     - 6 consistently achieved both.

    # Selected window of years.
    year_max_best = tb_change["year_max"].max()
    year_min_best = tb_change["year_min"].min() + max(1, ROLLING_AVERAGE_YEARS) - 1

    # Selected decoupled countries in that window.
    countries_decoupled = detect_decoupled_countries(
        tb_change=tb_change, year_min=year_min_best, year_max=year_max_best
    )

    # Select conly countries that were classified as decoupled.
    tb = tb[tb["country"].isin(countries_decoupled)].reset_index(drop=True)

    # # Create a simple table with the selected window of years for the selected countries.
    # tb_window = tb_change[
    #     (tb_change["country"].isin(countries_decoupled))
    #     & (tb_change["year_min"] == year_min_best)
    #     & (tb_change["year_max"] == year_max_best)
    # ].reset_index(drop=True)

    # # Remove unnecessary columns.
    # tb_window = tb_window.drop(columns=["year_min", "year_max", "n_years"], errors="raise")

    # # Set an appropriate index and sort conveniently.
    # tb_window = tb_window.format(["country"])

    ####################################################################################################################
    # TODO: Update these conclusions.
    # Conclusions:
    # This is the list of countries for which:
    # - Agricultural production increased by >= 5% in the latest year with respect to 1961.
    # - Agricultural land use decreased by >= 5% in the latest year with respect to 1961.
    # Hong Kong, Cyprus, Italy, Greece, Poland, Sweden, Hungary, Austria, New Zealand, South Korea, Australia, Guyana, Netherlands, Iran, Chile, Mongolia, Spain, Eswatini, Finland, France, Denmark, United Kingdom, Jordan, Germany, Switzerland, Argentina, Uruguay, Romania, Kiribati, Taiwan, Iceland, Bulgaria, Algeria, United States, Albania, Canada
    # show_decoupled_countries(food_column="production_energy", tb_fbsc=tb_fbsc, tb_grouped=tb_grouped, min_food_pct_change=5, min_land_pct_change=5)

    # This is the list of countries for which:
    # - Food supply increased by >= 5% in the latest year with respect to 1961.
    # - Agricultural land use decreased by >= 5% in the latest year with respect to 1961.
    # - The median net import share over the last 10 years is < 20%.
    # Italy, Greece, Poland, Hungary, Austria, New Zealand, Australia, Guyana, Iran, Chile, Spain, Eswatini, France, Denmark, Germany, Argentina, Uruguay, Romania, Mauritius, Kiribati, Iceland, United States, Albania, Canada
    # show_decoupled_countries(food_column="food_energy", tb_fbsc=tb_fbsc, tb_grouped=tb_grouped, min_food_pct_change=5, min_land_pct_change=5, max_net_share_imports=20, max_net_share_median_over_years=10)

    # This is the list of countries for which:
    # - Food supply increased by >= 5% in the latest year with respect to 1961.
    # - Agricultural land use decreased by >= 5% in the latest year with respect to 1961.
    # - The net import share is < 20% every year.
    # Greece, Poland, Hungary, Austria, New Zealand, Australia, Guyana, Iran, Chile, Spain, Eswatini, France, Denmark, Germany, Argentina, Uruguay, Romania, Kiribati, Iceland, United States, Albania, Canada
    # show_decoupled_countries(food_column="food_energy", tb_fbsc=tb_fbsc, tb_grouped=tb_grouped, min_food_pct_change=5, min_land_pct_change=5, max_net_share_imports=20, max_net_share_median_over_years=None)
    ####################################################################################################################

    # Improve table formats.
    tb = tb.format(keys=["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_fbsc.metadata)
    ds_garden.save()
