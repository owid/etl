"""Combine dataset on coverage of emissions with the average prices of emissions covered by an ETS or a carbon tax."""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# It may happen that the data for the most recent year is incomplete.
# If so, define the following to be last year fully informed.
# LAST_INFORMED_YEAR = 2021
LAST_INFORMED_YEAR = None

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    "jurisdiction": "country",
    "year": "year",
    # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_all_jurco2_usd_k": "price_with_tax_or_ets_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_all_jurghg_usd_k": "price_with_tax_or_ets_weighted_by_share_of_ghg",
    # Emissions-weighted average price on emissions covered by an ETS.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_ets_jurco2_usd_k": "price_with_ets_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by an ETS.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_ets_jurghg_usd_k": "price_with_ets_weighted_by_share_of_ghg",
    # Emissions-weighted average price on emissions covered by a carbon tax.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_tax_jurco2_usd_k": "price_with_tax_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by a carbon tax.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_tax_jurghg_usd_k": "price_with_tax_weighted_by_share_of_ghg",
    # CO2 emissions covered by either a carbon tax or an ETS as a share of jurisdiction total CO2 emissions.
    "cov_all_co2_jurco2": "co2_with_tax_or_ets_as_share_of_co2",
    # CO2 emissions covered by either a carbon tax or an ETS as a share of jurisdiction total GHG emissions.
    "cov_all_co2_jurghg": "co2_with_tax_or_ets_as_share_of_ghg",
    # CO2 emissions covered by either carbon taxes or an ETS as a share of world total CO2 emissions.
    "cov_all_co2_wldco2": "co2_with_tax_or_ets_as_share_of_world_co2",
    # CO2 emissions covered by either carbon taxes or an ETS as a share of world total GHG emissions.
    "cov_all_co2_wldghg": "co2_with_tax_or_ets_as_share_of_world_ghg",
    # CO2 emissions covered by an ETS as a share of jurisdiction total CO2 emissions.
    "cov_ets_co2_jurco2": "co2_with_ets_as_share_of_co2",
    # CO2 emissions covered by an ETS as a share of jurisdiction total GHG emissions.
    "cov_ets_co2_jurghg": "co2_with_ets_as_share_of_ghg",
    # CO2 emissions covered by an ETS as a share of world total CO2 emissions.
    "cov_ets_co2_wldco2": "co2_with_ets_as_share_of_world_co2",
    # CO2 emissions covered by an ETS as a share of world total GHG emissions.
    "cov_ets_co2_wldghg": "co2_with_ets_as_share_of_world_ghg",
    # CO2 emissions covered by a carbon tax as a share of jurisdiction total CO2 emissions.
    "cov_tax_co2_jurco2": "co2_with_tax_as_share_of_co2",
    # CO2 emissions covered by a carbon tax as a share of jurisdiction total GHG emissions.
    "cov_tax_co2_jurghg": "co2_with_tax_as_share_of_ghg",
    # CO2 emissions covered by a carbon tax as a share of world total CO2 emissions.
    "cov_tax_co2_wldco2": "co2_with_tax_as_share_of_world_co2",
    # CO2 emissions covered by a carbon tax as a share of world total GHG emissions.
    "cov_tax_co2_wldghg": "co2_with_tax_as_share_of_world_ghg",
    # # Other variables that are only relevant when considering sub-country regions (that we ignore for now):
    # # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_all_supraco2_usd_k': 'price_with_tax_or_ets_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_all_supraghg_usd_k': 'price_with_tax_or_ets_weighted_by_share_of_country_ghg',
    # # Emissions-weighted average price on emissions covered by an ETS.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_ets_supraco2_usd_k': 'price_with_ets_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by an ETS.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_ets_supraghg_usd_k': 'price_with_ets_weighted_by_share_of_country_ghg',
    # # Emissions-weighted average price on emissions covered by a carbon tax.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_tax_supraco2_usd_k': 'price_with_tax_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by a carbon tax.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_tax_supraghg_usd_k': 'price_with_tax_weighted_by_share_of_country_ghg',
    # # CO2 emissions covered by either carbon taxes or an ETS as a share of national jurisdiction CO2 emissions.
    # 'cov_all_co2_supraco2': 'co2_with_tax_or_ets_as_share_of_country_co2',
    # # CO2 emissions covered by either carbon taxes or an ETS as a share of national jurisdiction GHG emissions.
    # 'cov_all_co2_supraghg': 'co2_with_tax_or_ets_as_share_of_country_ghg',
    # # CO2 emissions covered by an ETS as a share of national jurisdiction total CO2 emissions.
    # 'cov_ets_co2_supraco2': 'co2_with_ets_as_share_of_country_co2',
    # # CO2 emissions covered by an ETS as a share of national jurisdiction total GHG emissions.
    # 'cov_ets_co2_supraghg': 'co2_with_ets_as_share_of_country_ghg',
    # # CO2 emissions covered by a carbon tax as a share of national jurisdiction total CO2 emissions.
    # 'cov_tax_co2_supraco2': 'co2_with_tax_as_share_of_country_co2',
    # # CO2 emissions covered by a carbon tax as a share of national jurisdiction total GHG emissions.
    # 'cov_tax_co2_supraghg': 'co2_with_tax_as_share_of_country_ghg',
}


def sanity_check_inputs(tb_economy: Table, tb_coverage: Table) -> None:
    """Run sanity checks on the raw data from meadow.

    Parameters
    ----------
    tb_economy : Table
        Raw data from meadow on prices.
    tb_coverage : Table
        Raw data from meadow on coverage.

    """
    # The economy and coverage tables historically had the same jurisdiction set, but starting in
    # the v2026.1 release they drift slightly (e.g. economy adds HK/Macau; coverage retains
    # legacy "Dc"/"District of Columbia" duplicates). Surface the differences as warnings and let
    # the outer merge handle the row alignment.
    economy_only = set(tb_economy["jurisdiction"]) - set(tb_coverage["jurisdiction"])
    coverage_only = set(tb_coverage["jurisdiction"]) - set(tb_economy["jurisdiction"])
    if economy_only:
        log.warning(f"Jurisdictions in economy but not coverage: {sorted(economy_only)}")
    if coverage_only:
        log.warning(f"Jurisdictions in coverage but not economy: {sorted(coverage_only)}")
    extra_coverage_years = set(tb_coverage["year"]) - set(tb_economy["year"])
    if extra_coverage_years:
        log.warning(f"Coverage has years not present in economy: {sorted(extra_coverage_years)}")

    # If the last year in the data is the current year, or if the data for the last year is missing, raise a warning.
    for tb in [tb_economy, tb_coverage]:
        column = tb.columns[2]
        if (
            tb["year"].max() == int(paths.version.split("-")[0])
            or tb[["year", column]].groupby(["year"], observed=True).sum(min_count=1)[column].isnull().iloc[-1]
        ):
            log.warning("The last year in the data may be incomplete. Define LAST_INFORMED_YEAR.")


def sanity_check_outputs(tb_combined: Table) -> None:
    """Run sanity checks on the output table."""
    error = "There should be no columns with only nans."
    assert tb_combined.columns[tb_combined.isna().all()].empty, error
    error = "Country named 'World' should be included in the countries file."
    assert "World" in set(tb_combined["country"]), error

    # Warn if a country had a non-zero value in the prior year and dropped to zero in the latest
    # year — often a sign of a spurious or partial-year input that needs human review.
    last_year = tb_combined["year"].max()
    for column in tb_combined.drop(columns=["country", "year"]).columns:
        zero_now = set(tb_combined[(tb_combined["year"] == last_year) & (tb_combined[column] == 0)]["country"])
        nonzero_before = set(tb_combined[(tb_combined["year"] == last_year - 1) & (tb_combined[column] > 0)]["country"])
        dropped = sorted(zero_now & nonzero_before)
        if dropped:
            log.warning(f"Countries with '{column}' dropped to zero in {last_year}: {dropped}")


def plot_price_coverage_curve(tb: Table, year: int | None = None) -> None:
    """Plot a step curve of carbon price vs. cumulative share of global CO₂ emissions.

    For a given year, sorts countries by their emissions-weighted carbon price (descending) and
    plots a horizontal segment per country whose width is that country's share of global CO₂
    emissions covered by a carbon tax or ETS. Unpriced emissions form a flat zero tail out to
    x=1, so the area under the curve makes the global picture (a small high-priced sliver, a
    long low-priced tail, and a large unpriced gap) immediately legible.

    Expects the combined country-level table BEFORE `tb.format(...)` (i.e. with `country` and
    `year` as plain columns, and `co2_with_tax_or_ets_as_share_of_world_co2` already in
    percent — `run()` multiplies the coverage block by 100 before this point).

    Parameters
    ----------
    tb : Table
        Combined garden table after harmonization, before `format()`.
    year : int | None
        Year to plot. Defaults to the latest year present.
    """
    import plotly.graph_objects as go

    df = tb[
        [
            "country",
            "year",
            "price_with_tax_or_ets_weighted_by_share_of_co2",
            "co2_with_tax_or_ets_as_share_of_world_co2",
        ]
    ].copy()
    df = df[df["country"] != "World"]
    if year is None:
        year = int(df["year"].dropna().max())
    df = (
        df[
            (df["year"] == year)
            & (df["price_with_tax_or_ets_weighted_by_share_of_co2"].fillna(0) > 0)
            & (df["co2_with_tax_or_ets_as_share_of_world_co2"].fillna(0) > 0)
        ]
        .sort_values("price_with_tax_or_ets_weighted_by_share_of_co2", ascending=False)
        .reset_index(drop=True)
    )
    if df.empty:
        log.warning(f"No priced countries found for year {year}; nothing to plot.")
        return

    # Coverage column is in percent at this stage — convert to a 0–1 proportion of global CO₂.
    share = df["co2_with_tax_or_ets_as_share_of_world_co2"] / 100.0
    cum = share.cumsum().tolist()
    prices = df["price_with_tax_or_ets_weighted_by_share_of_co2"].tolist()

    # Step curve with hv shape: (0, p₁), (cum₁, p₂), ..., (cum_{N-1}, p_N), (cum_N, 0), (1, 0).
    xs = [0.0] + cum[:-1] + [cum[-1], 1.0]
    ys = prices + [0.0, 0.0]

    fig = go.Figure(
        go.Scatter(
            x=xs,
            y=ys,
            mode="lines",
            line_shape="hv",
            line=dict(color="#cf0a2c", width=2.5),
            hovertemplate="cum. share: %{x:.3f}<br>price: $%{y:.2f}/tCO2<extra></extra>",
            showlegend=False,
        )
    )
    # Per-country markers at each segment's midpoint, for country-level hover.
    seg_mid_x = [(0.0 if i == 0 else cum[i - 1]) + share.iloc[i] / 2 for i in range(len(df))]
    fig.add_trace(
        go.Scatter(
            x=seg_mid_x,
            y=prices,
            mode="markers",
            marker=dict(size=6, color="#cf0a2c"),
            customdata=df[["country", "co2_with_tax_or_ets_as_share_of_world_co2"]].values,
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "price: $%{y:.2f}/tCO2<br>"
                "share of global CO2: %{customdata[1]:.2f}%<extra></extra>"
            ),
            showlegend=False,
        )
    )
    fig.update_layout(
        title=f"Share of global CO₂ emissions covered by carbon pricing ({year})",
        xaxis_title="Proportion of global CO₂ emissions",
        yaxis_title="Carbon price (constant 2021 US$ per ton CO₂e)",
        xaxis=dict(range=[0, 1]),
        yaxis=dict(rangemode="tozero"),
        template="plotly_white",
    )
    fig.show()


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main tables.
    ds_meadow = paths.load_dataset("emissions_weighted_carbon_price")
    tb_economy = ds_meadow.read("emissions_weighted_carbon_price_economy")
    tb_coverage = ds_meadow.read("emissions_weighted_carbon_price_coverage")

    #
    # Process data.
    #
    # Sanity checks on raw data.
    sanity_check_inputs(tb_economy=tb_economy, tb_coverage=tb_coverage)

    # Convert all values in coverage to percentages (instead of fractions).
    tb_coverage.loc[:, [column for column in tb_coverage.columns if column not in ["jurisdiction", "year"]]] *= 100

    # Combine both tables.
    tb_combined = tb_economy.merge(tb_coverage, how="outer", on=["jurisdiction", "year"], short_name=paths.short_name)

    # Select and rename columns.
    tb_combined = tb_combined[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    # NOTE: In the file of excluded countries we add all sub-national regions. This way, if an actual country is added
    # or removed, we will be warned. But, if many sub-national regions are added and including them in the excluded
    # countries file becomes a problem, we can remove that file and impose below make_missing_countries_nan=True, and
    # drop nans.
    tb_combined = paths.regions.harmonize_names(tb_combined)

    # Remove sub-regions within a country.
    tb_combined = tb_combined.dropna(subset=["country"]).reset_index(drop=True)

    if LAST_INFORMED_YEAR is not None:
        # Keep only data points prior to (or at) a certain year.
        tb_combined = tb_combined[tb_combined["year"] <= LAST_INFORMED_YEAR].reset_index(drop=True)

    # Sanity checks on the output table.
    sanity_check_outputs(tb_combined)

    # Optional: render the carbon-price-vs-cumulative-emissions-covered step curve for a given year.
    # Uncomment for ad-hoc inspection — opens an interactive plotly figure, not part of the build output.
    # plot_price_coverage_curve(tb_combined, year=2024)

    # Improve table format.
    tb_combined = tb_combined.format(keys=["country", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined], default_metadata=ds_meadow.metadata)
    ds_garden.save()
