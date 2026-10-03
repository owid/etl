"""Combine dataset on coverage of emissions with the average prices of emissions covered by an ETS or a carbon tax."""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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


def add_prices_on_covered_emissions(tb_combined: Table) -> Table:
    """Add the average price paid on covered emissions for each instrument (tax, ETS, both).

    RFF only publishes economy-wide prices weighted by a country's total CO2 emissions (with or without coverage).
    In other words, their price equals [price per covered tonne] x [share of emissions covered],
    diluted by the share of emissions that pay nothing. To recover the actual price per
    covered tonne, we divide the RFF price by the coverage share.

    Can the two mechanisms overlap? Per the World Carbon Pricing Database documentation, countries typically design their carbon tax to exempt sectors that are already in an ETS, so emissions-level overlap is rare.
    Where overlap genuinely exists, RFF subtracts it from `cov_all_CO2_jurCO2`.
    """
    # Coverage shares should never exceed 100%.
    # (1% tolerance for rounding.)
    for col in [
        "co2_with_tax_as_share_of_co2",
        "co2_with_ets_as_share_of_co2",
        "co2_with_tax_or_ets_as_share_of_co2",
    ]:
        flagged = sorted(set(tb_combined[tb_combined[col] > 101]["country"]))
        assert not flagged, f"{col} > 100% for: {flagged}"

    # Average tax rate on emissions covered by a tax.
    tax_coverage = tb_combined["co2_with_tax_as_share_of_co2"] / 100
    tb_combined["price_with_tax_on_covered_co2"] = (
        tb_combined["price_with_tax_weighted_by_share_of_co2"] / tax_coverage.where(tax_coverage > 0)
    ).fillna(0)

    # Average ETS price on emissions covered by an ETS.
    ets_coverage = tb_combined["co2_with_ets_as_share_of_co2"] / 100
    tb_combined["price_with_ets_on_covered_co2"] = (
        tb_combined["price_with_ets_weighted_by_share_of_co2"] / ets_coverage.where(ets_coverage > 0)
    ).fillna(0)

    # Average price on emissions covered by a tax or an ETS.
    tax_or_ets_coverage = tb_combined["co2_with_tax_or_ets_as_share_of_co2"] / 100
    tb_combined["price_with_tax_or_ets_on_covered_co2"] = (
        tb_combined["price_with_tax_or_ets_weighted_by_share_of_co2"]
        / tax_or_ets_coverage.where(tax_or_ets_coverage > 0)
    ).fillna(0)

    return tb_combined


def sanity_check_inputs(tb_economy: Table, tb_coverage: Table) -> None:
    """Assert that the meadow tables have matching keys and plausible value ranges."""
    error = "Economy and coverage tables have different jurisdictions."
    assert set(tb_economy["jurisdiction"]) == set(tb_coverage["jurisdiction"]), error
    error = "Economy and coverage tables have different years."
    assert set(tb_economy["year"]) == set(tb_coverage["year"]), error
    error = "Duplicate (jurisdiction, year) rows."
    assert not tb_economy.duplicated(subset=["jurisdiction", "year"]).any(), error
    assert not tb_coverage.duplicated(subset=["jurisdiction", "year"]).any(), error

    # All coverage and price values must be non-negative.
    coverage_cols = [c for c in tb_coverage.columns if c not in ["jurisdiction", "year"]]
    price_cols = [c for c in tb_economy.columns if c not in ["jurisdiction", "year"]]
    error = "Negative coverage or price values found."
    assert tb_coverage[coverage_cols].min().min() >= 0, error
    assert tb_economy[price_cols].min().min() >= 0, error


def sanity_check_outputs(tb_combined: Table) -> None:
    """Run sanity checks on the output table."""
    error = "There should be no columns with only nans."
    assert tb_combined.columns[tb_combined.isna().all()].empty, error

    # Warn if a country had a non-zero value in the prior year and dropped to zero in the latest
    # year — often a sign of a spurious or partial-year input that needs human review.
    last_year = tb_combined["year"].max()
    for column in tb_combined.drop(columns=["country", "year"]).columns:
        zero_now = set(tb_combined[(tb_combined["year"] == last_year) & (tb_combined[column] == 0)]["country"])
        nonzero_before = set(tb_combined[(tb_combined["year"] == last_year - 1) & (tb_combined[column] > 0)]["country"])
        dropped = sorted(zero_now & nonzero_before)
        if dropped:
            log.warning(f"Countries with '{column}' dropped to zero in {last_year}: {dropped}")


def plot_price_coverage_curve(tb: Table) -> None:
    """Plot a step curve of carbon price vs. cumulative share of global CO2 emissions."""
    import plotly.graph_objects as go

    tb_plot = tb[tb["country"] != "World"][
        [
            "country",
            "year",
            "price_with_tax_or_ets_on_covered_co2",
            "co2_with_tax_or_ets_as_share_of_world_co2",
        ]
    ].reset_index(drop=True)
    year = int(tb_plot["year"].dropna().max())
    tb_plot = (
        tb_plot[
            (tb_plot["year"] == year)
            & (tb_plot["price_with_tax_or_ets_on_covered_co2"].fillna(0) > 0)
            & (tb_plot["co2_with_tax_or_ets_as_share_of_world_co2"].fillna(0) > 0)
        ]
        .sort_values("price_with_tax_or_ets_on_covered_co2", ascending=False)
        .reset_index(drop=True)
    )

    share = tb_plot["co2_with_tax_or_ets_as_share_of_world_co2"]
    cumulative_share = share.cumsum().tolist()
    prices = tb_plot["price_with_tax_or_ets_on_covered_co2"].tolist()

    # Step curve with hv shape: (0, p₁), (cumulative₁, p₂), ..., (cumulative_{N-1}, p_N), (cumulative_N, 0), (100, 0).
    xs = [0.0] + cumulative_share[:-1] + [cumulative_share[-1], 100.0]
    ys = prices + [0.0, 0.0]

    fig = go.Figure(
        go.Scatter(
            x=xs,
            y=ys,
            mode="lines",
            line_shape="hv",
            line=dict(color="#cf0a2c", width=2.5),
            hovertemplate="cumulative share: %{x:.2f}%<br>price: $%{y:.2f}/tCO2<extra></extra>",
            showlegend=False,
        )
    )
    # Per-country markers at each segment's midpoint, for country-level hover.
    seg_mid_x = [(0.0 if i == 0 else cumulative_share[i - 1]) + share.iloc[i] / 2 for i in range(len(tb_plot))]
    fig.add_trace(
        go.Scatter(
            x=seg_mid_x,
            y=prices,
            mode="markers",
            marker=dict(size=6, color="#cf0a2c"),
            customdata=tb_plot[["country", "co2_with_tax_or_ets_as_share_of_world_co2"]].values,
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
        xaxis_title="Share of global CO₂ emissions",
        yaxis_title="Carbon price (constant 2021 US$ per ton CO₂e)",
        xaxis=dict(range=[0, 100], ticksuffix="%"),
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
    sanity_check_inputs(tb_economy=tb_economy, tb_coverage=tb_coverage)

    # Convert all values in coverage to percentages (instead of fractions).
    tb_coverage.loc[:, [column for column in tb_coverage.columns if column not in ["jurisdiction", "year"]]] *= 100

    # Combine both tables.
    tb_combined = tb_economy.merge(tb_coverage, how="outer", on=["jurisdiction", "year"], short_name=paths.short_name)

    # Select and rename columns.
    tb_combined = tb_combined[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    # NOTE: We consider only countries, and exclude sub-national jurisdictions.
    tb_combined = paths.regions.harmonize_names(tb=tb_combined)

    # Since the 2026 release, they provide data since 1970 (for Mexico and World); but it's all made of zeros.
    error = "Expected pre-1989 data to start in 1970, only for Mexico and World, and be all zeros."
    assert set(tb_combined[tb_combined["year"] < 1989]["country"]) == {"Mexico", "World"}, error
    assert tb_combined["year"].min() == 1970, error
    assert tb_combined["year"].min() == 1970, error
    assert tb_combined[tb_combined["year"] < 1989].drop(columns=["country", "year"]).sum(axis=0).sum() == 0, error
    # Remove pre-1989 data (which covers only Mexico, and it's made of zeros).
    tb_combined = tb_combined[tb_combined["year"] >= 1989].reset_index(drop=True)

    # Derive the average price paid on covered emissions for each instrument.
    tb_combined = add_prices_on_covered_emissions(tb_combined)

    # Sanity checks on the output table.
    sanity_check_outputs(tb_combined=tb_combined)

    # Optional: render the carbon-price-vs-cumulative-emissions-covered step curve for a given year.
    # Uncomment for ad-hoc inspection — opens an interactive plotly figure, not part of the build output.
    # plot_price_coverage_curve(tb=tb_combined)

    # Improve table format.
    tb_combined = tb_combined.format(keys=["country", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined], default_metadata=ds_meadow.metadata)
    ds_garden.save()
