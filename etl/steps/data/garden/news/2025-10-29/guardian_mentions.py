"""Media attention to countries.

Indicators estimated:

- num_pages: Number of pages mentioning a country name.
- relative_pages: Share of pages mentioning a country name.
- relative_pages_excluded: Share of pages tagged with a country name. It excludes COUNTRIES_EXCLUDED from share-estimation.
"""

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Countries to exclude in some indicators
COUNTRIES_EXCLUDED = {
    "United States",
    "United Kingdom",
    "Australia",
}
# Index columns
COLUMN_INDEX = ["country", "year"]
# Years: Minimum and maximum of the 10-year average period.
YEAR_DEC_MAX = 2024
YEAR_DEC_MIN = YEAR_DEC_MAX - 9


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("guardian_mentions")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow.read("guardian_mentions")

    #
    # Process data.
    #
    ## Harmonize countries
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove expected NaNs
    tb = check_and_filter_nans(tb)

    ## Get relative values
    tb = add_relative_indicators(tb, ["num_pages"])

    ## Add data for regions
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        regions=[
            "Europe",
            "Asia",
            "Africa",
            "North America",
            "South America",
            "Oceania",
            "North America (WB)",
            "Latin America and Caribbean (WB)",
            "Middle East, North Africa, Afghanistan and Pakistan (WB)",
            "Sub-Saharan Africa (WB)",
            "Europe and Central Asia (WB)",
            "South Asia (WB)",
            "East Asia and Pacific (WB)",
        ],
    )

    ## Add per-capita indicators
    tb = geo.add_population_to_table(tb, ds_population)
    tb["num_pages_per_million"] = tb["num_pages"] / tb["population"] * 1_000_000
    tb = tb.drop(columns="population")

    # Estimate 10-year average
    tb_10y_avg, tb_10y_avg_log = make_decadal_avg_table(tb)

    # DEV only: compare with old data
    # tb_max = format_maxroser(tb_10y_avg, ds_regions, ds_income_groups)
    # _compare_with_old(tb_max, drop_non_countries=True, num_countries=30)

    ## Format
    tb = tb.format(COLUMN_INDEX)
    tb_10y_avg = tb_10y_avg.format(["country", "year"], short_name="avg_10y")
    tb_10y_avg_log = tb_10y_avg_log.format(["country", "year"], short_name="avg_log_10y")

    tables = [
        tb,
        tb_10y_avg,
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_relative_indicators(tb: Table, colnames):
    """Add relative indicators (including excluding versions)"""
    for colname in colnames:
        tb = add_relative_indicator(tb, colname)
    return tb


def add_relative_indicator(tb, colname):
    """Add relative indicator.

    E.g. Global share of ? for a given year. Note that we use 'per 100,000' factor.
    """
    # Add relative indicator
    colname_new = f"{colname.replace('num_', 'relative_')}"
    tb.loc[:, colname_new] = get_relative_indicator(tb, colname).tolist()
    tb[colname_new].metadata.origins = tb[colname].metadata.origins

    # Add relative indicator excluding some countries
    tb_exc = tb.loc[~tb["country"].isin(COUNTRIES_EXCLUDED)].copy()
    colname_excluded = f"{colname.replace('num_', 'relative_')}_excluded"
    tb_exc[colname_excluded] = get_relative_indicator(tb_exc, colname).tolist()
    tb = tb.merge(tb_exc[COLUMN_INDEX + [colname_excluded]], on=COLUMN_INDEX, how="left")
    tb[colname_excluded].metadata.origins = tb[colname].metadata.origins

    # Ensure no NA in columns but excluded
    cols = [col for col in tb.columns if col != colname_excluded]
    assert not tb[cols].isna().any().any(), f"NA values found in unexpected columns {cols}"
    countries_with_nas = set(tb.loc[tb[colname_excluded].isna(), "country"].unique())
    assert (
        countries_with_nas == COUNTRIES_EXCLUDED
    ), f"Unexpected countries with NA in {colname_excluded}: {countries_with_nas}"

    return tb


def get_relative_indicator(tb, colname):
    """Add relative indicator.

    E.g. Global share of ? for a given year. Note that we use 'per 100,000' factor.
    """
    tb_ = tb.copy()

    tb_total = tb_[tb_["country"] == "Total"]
    tb_ = tb_.merge(tb_total[["year", colname]], on="year", suffixes=["", "_total"])

    return tb_[colname] / tb_[f"{colname}_total"] * 100_000

    # tb[f"{colname.replace('num_', 'relative_')}"] = tb[colname] / tb["total"] * 100_000
    # tb = tb.drop(columns=["total"])
    # return tb


def check_and_filter_nans(tb):
    nas_country = tb[tb["num_pages"].isna()].groupby("country").size().sort_values().to_dict()
    nas_country_expected = {
        "Aland Islands": 1,
        "Cocos Islands": 1,
        "Sao Tome and Principe": 1,
        "Niue": 1,
        "Tokelau": 2,
        "Wallis and Futuna": 4,
        "Heard Island and McDonald Islands": 6,
        "Saint Pierre and Miquelon": 6,
        "Bouvet Island": 12,
        "French Southern Territories": 12,
    }
    assert nas_country == nas_country_expected, f"Unexpected NaNs in countries: {nas_country}"

    return tb.dropna(subset=["num_pages"])


def make_decadal_avg_table(tb: Table):
    """Get table with 10-year average and log(10-year average) indicators."""
    tb_10y_avg = tb[(tb["year"] >= YEAR_DEC_MIN) & (tb["year"] <= YEAR_DEC_MAX)].copy()
    tb_10y_avg = tb_10y_avg.rename(
        columns={col: f"{col}_10y_avg" for col in tb_10y_avg.columns if col not in COLUMN_INDEX}
    )
    columns_indicators = [col for col in tb_10y_avg.columns if col not in COLUMN_INDEX]
    tb_10y_avg = tb_10y_avg.groupby("country", observed=False)[columns_indicators].mean().reset_index()
    tb_10y_avg["year"] = YEAR_DEC_MAX

    # Copy metadata
    col_og = [col for col in tb.columns if col not in COLUMN_INDEX][0]
    for col in [col for col in tb_10y_avg.columns if col not in COLUMN_INDEX]:
        tb_10y_avg[col] = tb_10y_avg[col].copy_metadata(tb[col_og])

    # Estimate log(10-year average)
    tb_10y_avg_log = tb_10y_avg.copy()
    tb_10y_avg_log[columns_indicators] = np.log(tb_10y_avg[columns_indicators] + 1)
    tb_10y_avg_log = tb_10y_avg_log.rename(columns={col: f"{col}_10y_avg_log" for col in columns_indicators})
    tb_10y_avg_log["year"] = YEAR_DEC_MAX

    return tb_10y_avg, tb_10y_avg_log


######################################################################
# CODE FOR ADHOC ANALYSIS FOR MAX
#
# Formatting data to be used by Max
# Comparing new data with old data
######################################################################


def format_maxroser(tb: Table, ds_regions, ds_income_groups) -> Table:
    # Keep relevant columns
    COLUMNS_INDEX = ["country", "year"]
    COLUMNS_INDICATORS = ["num_pages_10y_avg", "relative_pages_10y_avg"]
    tb = tb.loc[:, COLUMNS_INDEX + COLUMNS_INDICATORS]

    # Add codes
    tb_regions = ds_regions["regions"][["name", "iso_alpha3"]].rename(
        columns={
            "name": "country",
            "iso_alpha3": "code",
        }
    )
    tb = tb.merge(tb_regions, on=["country"], how="left")

    # Dtypes
    tb = tb.astype(
        {
            "country": "string",
            "code": "string",
        }
    )

    # Missing codes
    tb.loc[tb["country"] == "Kosovo", "code"] = "OWID_KOS"
    ## Sanity check remaining missing codes
    missing_codes = set(tb[tb["code"].isna()]["country"].unique())
    missing_codes_expected = {
        "Africa",
        "Asia",
        "East Asia and Pacific (WB)",
        "Europe",
        "Europe and Central Asia (WB)",
        "Latin America and Caribbean (WB)",
        "Middle East, North Africa, Afghanistan and Pakistan (WB)",
        "North America",
        "North America (WB)",
        "Oceania",
        "South America",
        "South Asia (WB)",
        "Sub-Saharan Africa (WB)",
        "Total",
    }
    assert set(missing_codes) == missing_codes_expected, f"Missing codes for countries: {missing_codes}"

    # Drop regions
    tb = tb.loc[~tb["country"].isin(missing_codes - {"Total"})]

    # Add regions
    c2continent = geo.countries_to_continent_mapping(
        ds_regions=ds_regions,
    )
    c2income = geo.countries_to_continent_mapping(
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=geo.INCOME_GROUPS,
    )
    tb["continent"] = tb["country"].map(c2continent)
    tb["wb_income_group"] = tb["country"].map(c2income)

    # Rounding decimals
    tb[COLUMNS_INDICATORS] = tb[COLUMNS_INDICATORS].round(3)

    # Order columns
    tb = tb.loc[:, COLUMNS_INDEX + ["code", "continent", "wb_income_group"] + COLUMNS_INDICATORS]

    return tb


def load_old_data(path_num, path_relative) -> pd.DataFrame:
    """Load old data to compare with new estimates."""

    df_num = pd.read_csv(path_num)
    df_relative = pd.read_csv(path_relative)

    shape1 = df_num.shape
    shape2 = df_relative.shape

    df = df_num.merge(
        df_relative,
        on=["Entity", "Code", "Year"],
    )

    assert shape1[0] == shape2[0] == df.shape[0], "Mismatch in number of rows between datasets."
    df = df.rename(
        columns={
            "Entity": "country",
            "Year": "year",
            "Code": "code",
            "Number of pages in the Guardian that mention a country (10-year average)": "num_pages_10y_avg",
            "Share of pages in The Guardian that mention a country (10-year average)": "relative_pages_10y_avg",
        }
    )

    df["country"] = df["country"].replace(
        {
            "Middle East and North Africa (WB)": "Middle East, North Africa, Afghanistan and Pakistan (WB)",
            "Northern Marianas Islands": "Northern Mariana Islands",
        }
    )
    return df


def _compare_with_old(tb_max, drop_non_countries=False, num_countries=None, output=None):
    # Load old data
    path_num = ""
    path_relative = ""
    df = load_old_data(path_num, path_relative)

    # Drop year
    tb_max = tb_max.drop(columns=["year"])
    df = df.drop(columns=["year"])

    # Merge
    df_diff = df.merge(tb_max, on=["country", "code"], suffixes=("_old", "_new"), how="outer")
    df_diff["num_diff"] = df_diff["num_pages_10y_avg_new"] - df_diff["num_pages_10y_avg_old"]
    df_diff["num_diff_abs"] = df_diff["num_diff"].abs()
    df_diff["relative_diff"] = df_diff["relative_pages_10y_avg_new"] - df_diff["relative_pages_10y_avg_old"]
    df_diff["relative_diff_abs"] = df_diff["relative_diff"].abs()

    # Optionally drop non-countries
    if drop_non_countries:
        df_diff = df_diff[~df_diff["code"].isna()]
    # TODO: I want to generate two plots to compare old vs new data. We have two main indicator types that we want to compare (counts and relative shares), so we will make two plots. I want both plots to show linecharts for old and new data, with country as the x-axis, displayable on hover.
    #
    # Countries in x-axis should be sorted by the absolute difference between old and new data (*_diff_abs).
    #
    # The plot should also show a bar with the difference (positive or negative) between old and new data for each country. Have it in a lighter color so it doesn't distract from the main lines.
    #
    # Main lines should be blue for old and red for new.
    #
    # Have the plot be interactive, maybe with plotly. Then i'll export as HTML to be shared.
    #
    # Plot 1
    ## indicators: num_pages_10y_avg_old, num_pages_10y_avg_new
    ## countries: country
    ## difference: num_diff
    ## difference_abs: num_diff_abs
    #
    # Plot 2
    ## indicators: relative_pages_10y_avg_old, relative_pages_10y_avg_new
    ## countries: country
    ## difference: relative_diff
    ## difference_abs: relative_diff_abs

    return _plot_comparison(df_diff, num_countries=num_countries, output_path=output)


def _plot_comparison(df, output_path=None, num_countries=None):
    """Generate interactive plots comparing old vs new data.

    Args:
        df: DataFrame with old and new data
        output_path: Optional path to save HTML file. If None, only displays the plot.

    Returns:
        Plotly figure object
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Create two subplots (one for counts, one for relative shares)
    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=(
            "Number of Pages (10-year average)",
            "Relative Share of Pages (10-year average)",
        ),
        vertical_spacing=0.15,
    )

    # Plot 1: Counts comparison
    df_sorted_1 = df.sort_values("num_diff_abs", ascending=False).reset_index(drop=True)
    if num_countries:
        df_sorted_1 = df_sorted_1.head(num_countries)

    # Use country names on x-axis if num_countries is specified, otherwise use index
    x_values_1 = df_sorted_1["code"] if num_countries else df_sorted_1.index

    # Prepare custom data with all information for hover
    customdata_1 = df_sorted_1[
        [
            "country",
            "num_pages_10y_avg_old",
            "num_pages_10y_avg_new",
            "num_diff",
            "relative_pages_10y_avg_old",
            "relative_pages_10y_avg_new",
            "relative_diff",
        ]
    ].values

    hover_template = (
        "<b>%{customdata[0]}</b><br><br>"
        + "<b>Number of Pages:</b><br>"
        + "Old: %{customdata[1]:.2f}<br>"
        + "New: %{customdata[2]:.2f}<br>"
        + "Difference: %{customdata[3]:.2f}<br><br>"
        + "<b>Relative Share:</b><br>"
        + "Old: %{customdata[4]:.2f}<br>"
        + "New: %{customdata[5]:.2f}<br>"
        + "Difference: %{customdata[6]:.2f}"
        + "<extra></extra>"
    )

    # Add old data line (blue)
    fig.add_trace(
        go.Scatter(
            x=x_values_1,
            y=df_sorted_1["num_pages_10y_avg_old"],
            mode="lines+markers",
            name="Old data (counts)",
            line=dict(color="blue", width=2),
            marker=dict(size=4),
            hovertemplate=hover_template,
            customdata=customdata_1,
        ),
        row=1,
        col=1,
    )

    # Add new data line (red)
    fig.add_trace(
        go.Scatter(
            x=x_values_1,
            y=df_sorted_1["num_pages_10y_avg_new"],
            mode="lines+markers",
            name="New data (counts)",
            line=dict(color="red", width=2),
            marker=dict(size=4),
            hovertemplate=hover_template,
            customdata=customdata_1,
        ),
        row=1,
        col=1,
    )

    # Plot 2: Relative shares comparison
    df_sorted_2 = df.sort_values("relative_diff_abs", ascending=False).reset_index(drop=True)
    df_sorted_2 = df_sorted_2[df_sorted_2["country"] != "Total"]
    if num_countries:
        df_sorted_2 = df_sorted_2.head(num_countries)

    # Use country names on x-axis if num_countries is specified, otherwise use index
    x_values_2 = df_sorted_2["code"] if num_countries else df_sorted_2.index

    # Prepare custom data with all information for hover
    customdata_2 = df_sorted_2[
        [
            "country",
            "num_pages_10y_avg_old",
            "num_pages_10y_avg_new",
            "num_diff",
            "relative_pages_10y_avg_old",
            "relative_pages_10y_avg_new",
            "relative_diff",
        ]
    ].values

    hover_template_2 = (
        "<b>%{customdata[0]}</b><br><br>"
        + "<b>Number of Pages:</b><br>"
        + "Old: %{customdata[1]:.2f}<br>"
        + "New: %{customdata[2]:.2f}<br>"
        + "Difference: %{customdata[3]:.2f}<br><br>"
        + "<b>Relative Share:</b><br>"
        + "Old: %{customdata[4]:.2f}<br>"
        + "New: %{customdata[5]:.2f}<br>"
        + "Difference: %{customdata[6]:.2f}"
        + "<extra></extra>"
    )

    # Add old data line (blue)
    fig.add_trace(
        go.Scatter(
            x=x_values_2,
            y=df_sorted_2["relative_pages_10y_avg_old"],
            mode="lines+markers",
            name="Old data (relative)",
            line=dict(color="blue", width=2),
            marker=dict(size=4),
            hovertemplate=hover_template_2,
            customdata=customdata_2,
        ),
        row=2,
        col=1,
    )

    # Add new data line (red)
    fig.add_trace(
        go.Scatter(
            x=x_values_2,
            y=df_sorted_2["relative_pages_10y_avg_new"],
            mode="lines+markers",
            name="New data (relative)",
            line=dict(color="red", width=2),
            marker=dict(size=4),
            hovertemplate=hover_template_2,
            customdata=customdata_2,
        ),
        row=2,
        col=1,
    )

    # Update layout
    fig.update_layout(
        height=1200,
        title_text="Guardian Mentions: Old vs New Data Comparison",
        showlegend=True,
        hovermode="closest",
    )

    # Update x-axes
    if num_countries:
        # When showing country names, rotate labels for readability
        fig.update_xaxes(
            title_text="Countries (sorted by absolute difference)",
            tickangle=-45,
            row=1,
            col=1,
        )
        fig.update_xaxes(
            title_text="Countries (sorted by absolute difference)",
            tickangle=-45,
            row=2,
            col=1,
        )
    else:
        fig.update_xaxes(title_text="Countries (sorted by absolute difference)", row=1, col=1)
        fig.update_xaxes(title_text="Countries (sorted by absolute difference)", row=2, col=1)

    # Update y-axes
    fig.update_yaxes(title_text="Number of Pages", row=1, col=1)
    fig.update_yaxes(title_text="Relative Share", row=2, col=1)

    # Save as HTML if path provided
    if output_path:
        fig.write_html(output_path)
        print(f"Plot saved to: {output_path}")

    # Display the plot (works in notebooks)
    # Try different renderers for VS Code compatibility
    try:
        fig.show(renderer="plotly_mimetype+notebook")
    except Exception:
        try:
            fig.show(renderer="notebook")
        except Exception:
            # Fallback: just return the figure (VS Code will render it automatically)
            pass

    return fig
