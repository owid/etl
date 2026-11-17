"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import structlog
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = geo.REGIONS

LOG = structlog.get_logger()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_igme = paths.load_dataset("igme")
    ds_gapminder_v11 = paths.load_dataset("under_five_mortality", version="2023-09-21")
    ds_gapminder_v7 = paths.load_dataset("under_five_mortality", version="2023-09-18")
    ds_unwpp = paths.load_dataset("un_wpp")

    # Read UN WPP under-five mortality without its templated metadata (it's described in this step's .meta.yml).
    tb_un_wpp_cm = ds_unwpp.read("mortality_rate", reset_metadata="keep_origins")
    tb_un_wpp_cm = tb_un_wpp_cm[
        (tb_un_wpp_cm["age"] == "0-4") & (tb_un_wpp_cm["variant"] == "estimates") & (tb_un_wpp_cm["sex"] == "all")
    ]

    tb_un_wpp_births = ds_unwpp.read("births")
    tb_un_wpp_births = tb_un_wpp_births[
        (tb_un_wpp_births["age"] == "all") & (tb_un_wpp_births["variant"] == "estimates")
    ][["country", "year", "births"]]

    tb_pop = ds_unwpp.read("population")
    tb_pop = tb_pop[
        ((tb_pop["age"] == "all") | (tb_pop["age"] == "0-4"))
        & (tb_pop["variant"] == "estimates")
        & (tb_pop["sex"] == "all")
    ][["country", "year", "age", "population"]]

    # Perform sanity checks on input datasets
    _validate_input_datasets(ds_igme, ds_gapminder_v11, ds_gapminder_v7)

    # Read table from meadow dataset and filter out the main indicator, under five mortality, central estimate, both sexes, all wealth quintiles.
    tb_igme = ds_igme.read("igme", reset_metadata="keep_origins")
    tb_igme = tb_igme[
        (tb_igme["indicator"] == "Child mortality rate")
        & (tb_igme["sex"] == "Total")
        & (tb_igme["wealth_quintile"] == "Total")
    ]
    assert len(tb_igme) > 0, "Check you are using the right terms to filter the data"
    tb_igme = tb_igme.rename(columns={"observation_value": "child_mortality_rate"}, errors="raise").drop(
        columns=["sex", "wealth_quintile", "indicator", "unit_of_measure"]
    )
    # Select out columns of interest.
    tb_igme["source"] = "UN IGME"
    tb_igme["source_url"] = "https://childmortality.org/data/igme/"
    # Load full Gapminder data v11, v11 includes projections, so we need to remove years beyond the last year of IGME data

    max_year = tb_igme["year"].max()
    tb_gap_full = ds_gapminder_v11["under_five_mortality"].reset_index()
    tb_gap_full = tb_gap_full[tb_gap_full["year"] <= max_year].reset_index(drop=True)
    tb_gap_full = tb_gap_full.rename(columns={"child_mortality": "child_mortality_rate"}, errors="raise")
    tb_gap_full["source"] = "Gapminder v11"
    tb_gap_full["source_url"] = (
        "https://docs.google.com/spreadsheets/d/1Av7eps_zEK73-AdbFYEmtTrwFKlfruBYXdrnXAOFVpM/edit?gid=501532268#gid=501532268"
    )
    tb_gap_full["child_mortality_rate"] = tb_gap_full["child_mortality_rate"].div(10)

    # Load Gapminder data v7 - has the source of the data (unlike v11)
    # We've removed some years from the v7 data, for years where the source was 'Guesstimate' or 'Model based on Life Expectancy'
    tb_gap_sel = ds_gapminder_v7["under_five_mortality_selected"].reset_index()
    tb_gap_sel["source"] = "Gapminder v7"
    tb_gap_sel["source_url"] = "https://www.gapminder.org/documentation/documentation/gapdata005%20v7.xlsx"
    tb_gap_sel = tb_gap_sel.rename(columns={"under_five_mortality": "child_mortality_rate"}, errors="raise")
    tb_gap_sel["child_mortality_rate"] = tb_gap_sel["child_mortality_rate"].div(10)
    # Remove the early years for Austria - there is a signicant jump in the data in 1830 which suggests an incongruency in method or data availability
    tb_gap_sel = remove_early_years_austria(tb_gap_sel)
    # Add 'World' from full Gapminder dataset to selected Gapminder dataset.
    tb_gap_sel = add_world_from_gapminder_full_to_selected(tb_gap_full, tb_gap_sel)
    # Combine IGME and Gapminder data with two versions

    tb_combined_full = combine_datasets(tb_igme, tb_gap_full, "long_run_child_mortality")
    tb_combined_sel = combine_datasets(tb_igme, tb_gap_sel, "long_run_child_mortality_selected")

    # Calculate and estimate globally the number of children surviving their first five years
    tb_surviving = calculate_share_surviving_first_five_years(tb_combined_full)
    # Combine with full Gapminder dataset
    tb_combined_full = pr.merge(tb_combined_full, tb_surviving, on=["country", "year"], how="left")
    tb_combined_full = tb_combined_full.rename(
        columns={"child_mortality_rate": "child_mortality_rate_full"}, errors="raise"
    )

    # Perform sanity checks on final output data
    _validate_output_data(tb_combined_full, tb_combined_sel)

    # Save outputs.
    tb_combined_full = tb_combined_full.drop(columns=["source"]).format(["country", "year"])
    tb_combined_sel = tb_combined_sel.format(["country", "year"])
    tb_combined_sel["source"].metadata.origins = tb_combined_sel["child_mortality_rate"].metadata.origins
    tb_combined_sel["source_url"].metadata.origins = tb_combined_sel["child_mortality_rate"].metadata.origins

    # calculate regional averages from selected data:
    tb_regional_averages = calculate_regional_averages(tb_combined_sel, tb_pop, tb_un_wpp_births)
    tb_regional_averages = tb_regional_averages.format(
        ["country", "year"], short_name="long_run_child_mortality_regions"
    )

    # Keep UN WPP under-five mortality as a separate table, for comparison.
    tb_un_wpp_cm = (
        tb_un_wpp_cm[["country", "year", "mortality_rate"]]
        .rename(columns={"mortality_rate": "child_mortality_rate"}, errors="raise")
        .format(["country", "year"], short_name="un_wpp_child_mortality")
    )

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_combined_full, tb_combined_sel, tb_regional_averages, tb_un_wpp_cm], check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_regional_averages(
    tb_data: Table,
    tb_pop: Table,
    tb_births: Table,
    min_frac_population: float = 0.8,
    year_min: int = 1950,
) -> Table:
    """Calculate regional child mortality rates for all OWID regions, with three different weights.

    Returns a table with only region rows and three columns:
    - child_mortality_rate_weighted_population: weighted by total population.
    - child_mortality_rate_weighted_population_0_4: weighted by the population aged 0-4.
    - child_mortality_rate_weighted_births: weighted by births in the same year.

    A region-year is left empty when the countries with data cover less than `min_frac_population` of the region's
    total population. Only years from `year_min` onwards are kept, since UN WPP weights start in 1950.
    """
    # All OWID regions (continents and income groups), plus World, which geo.REGIONS doesn't include.
    regions = list(REGIONS) + ["World"]
    weights = {
        "population": "population_total",
        "population_0_4": "population_0_4",
        "births": "births",
    }

    # Keep only countries (regions are recomputed below) and years with UN WPP weights.
    tb = tb_data.reset_index()[["country", "year", "child_mortality_rate"]]
    tb = tb[~tb["country"].isin(regions) & (tb["year"] >= year_min)].dropna(subset=["child_mortality_rate"])

    # Add the three weights as columns.
    tb_pop_total = tb_pop[tb_pop["age"] == "all"][["country", "year", "population"]].rename(
        columns={"population": "population_total"}
    )
    tb_pop_0_4 = tb_pop[tb_pop["age"] == "0-4"][["country", "year", "population"]].rename(
        columns={"population": "population_0_4"}
    )
    tb = pr.merge(tb, tb_pop_total, on=["country", "year"], how="left")
    tb = pr.merge(tb, tb_pop_0_4, on=["country", "year"], how="left")
    tb = pr.merge(tb, tb_births[["country", "year", "births"]], on=["country", "year"], how="left")

    # One copy of the rate per weight. Blank the copy where its weight is missing, so that country neither enters the
    # weighted mean nor counts as covered in the population threshold.
    aggregations = {}
    for name, weight_col in weights.items():
        col = f"child_mortality_rate_weighted_{name}"
        tb[col] = tb["child_mortality_rate"].copy()
        tb.loc[tb[weight_col].isna(), col] = None
        aggregations[col] = f"mean_weighted_by_{weight_col}"
    # Weight columns are summed (they're needed as columns for the weighted means, but dropped afterwards).
    aggregations.update({weight_col: "sum" for weight_col in weights.values()})

    # Calculate weighted regional averages, dropping region-years with less than 80% population coverage.
    tb = paths.regions.add_aggregates(
        tb.drop(columns=["child_mortality_rate"]),
        regions=regions,
        aggregations=aggregations,
        population_col="population_total",
        min_frac_population=min_frac_population,
    )

    # Keep only regions and the weighted rates.
    tb = tb[tb["country"].isin(regions)].drop(columns=list(weights.values()))
    rate_cols = [f"child_mortality_rate_weighted_{name}" for name in weights]
    tb = tb.dropna(subset=rate_cols, how="all").reset_index(drop=True)

    return tb


def add_world_from_gapminder_full_to_selected(tb_gap_full: Table, tb_gap_sel: Table) -> Table:
    """
    The 'full' Gapminder dataset has a much longer time series for the world than the 'selected' Gapminder dataset.
    We don't tend to promote the 'full' Gapminder dataset as it has a lot of guesses, but as should the global total in this chart - https://ourworldindata.org/grapher/global-child-mortality-timeseries,
    so I think we can also show it in the 'selected' dataset.
    """
    tb_gap_full = tb_gap_full[tb_gap_full["country"] == "World"]
    tb_gap_sel = tb_gap_sel[tb_gap_sel["country"] != "World"]

    tb = pr.concat([tb_gap_sel, tb_gap_full], ignore_index=True)
    return tb


def combine_datasets(tb_igme: Table, tb_gap: Table, table_name: str) -> Table:
    """
    Combine IGME and Gapminder data.
    """
    tb_combined = pr.concat([tb_igme, tb_gap]).sort_values(["country", "year", "source"])
    tb_combined.metadata.short_name = table_name
    tb_combined = remove_duplicates(tb_combined, preferred_source="UN IGME")

    return tb_combined


def remove_early_years_austria(tb: Table) -> Table:
    """
    Remove years prior to 1830 for Austria - there is a signicant jump in the data in 1830 which suggests an incongruency in method or data availability
    """
    # Remove years prior to 1830 for Austria as they are likely an error
    msk = (tb["country"] == "Austria") & (tb["year"] < 1830)
    tb = tb[~msk]

    return tb


def remove_duplicates(tb: Table, preferred_source: str) -> Table:
    """
    Removing rows where there are overlapping years with a preference for IGME data.

    """
    assert tb["source"].str.contains(preferred_source).any()

    duplicate_rows = tb.duplicated(subset=["country", "year"], keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed])

    assert len(tb[tb.duplicated(subset=["country", "year"], keep=False)]) == 0

    return tb


def calculate_share_surviving_first_five_years(tb_combined: Table) -> Table:
    """
    Calculate and estimate globally the number of children surviving their first five years.
    """
    # Drop out years prior to 1800 and regions that aren't countries

    tb_world = tb_combined[(tb_combined["country"] == "World")].drop(columns=["source", "source_url"], errors="ignore")

    # Add global labels and calculate the share of children surviving/dying in their first five years

    tb_world["share_dying_first_five_years"] = tb_world["child_mortality_rate"]
    tb_world["share_surviving_first_five_years"] = 100 - (tb_world["child_mortality_rate"])

    tb_world = tb_world.drop(columns=["child_mortality_rate"])

    return tb_world


def _validate_input_datasets(ds_igme, ds_gapminder_v11, ds_gapminder_v7) -> None:
    """Validate input datasets for basic integrity checks."""
    # Check that datasets are not empty
    assert len(ds_igme) > 0, "IGME dataset is empty"
    assert len(ds_gapminder_v11) > 0, "Gapminder v11 dataset is empty"
    assert len(ds_gapminder_v7) > 0, "Gapminder v7 dataset is empty"

    # Check IGME data has expected columns
    tb_igme = ds_igme.read("igme")
    required_igme_cols = ["indicator", "sex", "wealth_quintile", "observation_value", "country", "year"]
    missing_cols = [col for col in required_igme_cols if col not in tb_igme.columns]
    assert not missing_cols, f"IGME dataset missing required columns: {missing_cols}"

    # Check Gapminder data has expected columns
    tb_gap_v11 = ds_gapminder_v11["under_five_mortality"]
    tb_gap_v7 = ds_gapminder_v7["under_five_mortality_selected"]

    assert "child_mortality" in tb_gap_v11.columns, "Gapminder v11 missing 'child_mortality' column"
    assert "under_five_mortality" in tb_gap_v7.columns, "Gapminder v7 missing 'under_five_mortality' column"
    assert "country" in tb_gap_v11.reset_index().columns, "Gapminder v11 missing 'country' column"
    assert "year" in tb_gap_v11.reset_index().columns, "Gapminder v11 missing 'year' column"


def _validate_output_data(tb_combined_full: Table, tb_combined_sel: Table) -> None:
    """Validate final output data for sanity checks."""
    # Check child mortality rate ranges (should be 0-100%)
    full_rates = tb_combined_full["child_mortality_rate_full"]
    sel_rates = tb_combined_sel["child_mortality_rate"]

    # Check for reasonable mortality rate bounds
    assert full_rates.min() >= 0, f"Negative child mortality rate found: {full_rates.min()}"
    assert full_rates.max() <= 100, f"Child mortality rate exceeds 100%: {full_rates.max()}"
    assert sel_rates.min() >= 0, f"Negative child mortality rate found: {sel_rates.min()}"
    assert sel_rates.max() <= 100, f"Child mortality rate exceeds 100%: {sel_rates.max()}"

    # Check survival rates are complementary to mortality rates (within 0.1% tolerance)
    world_data = tb_combined_full[tb_combined_full["country"] == "World"]
    if len(world_data) > 0:
        mortality_rates = world_data["child_mortality_rate_full"]
        survival_rates = world_data["share_surviving_first_five_years"]
        expected_survival = 100 - mortality_rates
        rate_diff = abs(survival_rates - expected_survival)
        assert rate_diff.max() < 0.1, f"Survival rates don't match mortality rates (max diff: {rate_diff.max()}%)"

    # Check year ranges are reasonable
    min_year, max_year = 1751, 2025
    full_years = tb_combined_full["year"]
    sel_years = tb_combined_sel["year"]

    assert full_years.min() >= min_year, f"Year too early in full data: {full_years.min()}"
    assert full_years.max() <= max_year, f"Year too late in full data: {full_years.max()}"
    assert sel_years.min() >= min_year, f"Year too early in selected data: {sel_years.min()}"
    assert sel_years.max() <= max_year, f"Year too late in selected data: {sel_years.max()}"

    # Check for required countries/regions
    full_countries = set(tb_combined_full["country"].unique())
    sel_countries = set(tb_combined_sel["country"].unique())

    assert "World" in full_countries, "World data missing from full dataset"
    assert "World" in sel_countries, "World data missing from selected dataset"

    # Check source consistency in selected dataset
    valid_sources = {"UN IGME", "Gapminder v11", "Gapminder v7"}
    sel_sources = set(tb_combined_sel["source"].unique())
    invalid_sources = sel_sources - valid_sources
    assert not invalid_sources, f"Invalid sources found: {invalid_sources}"

    # Check no duplicate country-year combinations
    full_dupes = tb_combined_full.duplicated(subset=["country", "year"]).sum()
    sel_dupes = tb_combined_sel.duplicated(subset=["country", "year"]).sum()
    assert full_dupes == 0, f"Found {full_dupes} duplicate country-year pairs in full dataset"
    assert sel_dupes == 0, f"Found {sel_dupes} duplicate country-year pairs in selected dataset"

    # Check that share variables are percentages (0-100)
    if "share_dying_first_five_years" in tb_combined_full.columns:
        dying_rates = tb_combined_full["share_dying_first_five_years"]
        assert dying_rates.min() >= 0, f"Negative dying share: {dying_rates.min()}"
        assert dying_rates.max() <= 100, f"Dying share exceeds 100%: {dying_rates.max()}"

    # Validate regional data consistency and reasonableness
    _validate_regional_data(tb_combined_full, tb_combined_sel)


def _validate_regional_data(tb_combined_full: Table, tb_combined_sel: Table) -> None:
    """Validate regional data for consistency and reasonable values."""
    expected_regions = {"World", "Asia", "Africa", "Europe", "North America", "South America", "Oceania"}

    # Check both datasets for regional coverage
    for tb, name in [(tb_combined_full, "full"), (tb_combined_sel, "selected")]:
        countries = set(tb["country"].unique())
        missing_regions = expected_regions - countries

        # Warn about missing regions (not all regions may be present in historical data)
        if missing_regions:
            LOG.warning(f"Missing regions in {name} dataset: {missing_regions}")

    # Validate regional mortality rates are reasonable based on historical patterns
    for tb, name in [(tb_combined_full, "full"), (tb_combined_sel, "selected")]:
        rate_col = "child_mortality_rate_full" if name == "full" else "child_mortality_rate"

        if rate_col not in tb.columns:
            continue

        regional_data = tb[tb["country"].isin(expected_regions)]

        if len(regional_data) == 0:
            continue

        # Check regional mortality rates are within historically observed bounds
        for region in expected_regions:
            region_data = regional_data[regional_data["country"] == region]

            if len(region_data) == 0:
                continue

            rates = region_data[rate_col].dropna()

            if len(rates) == 0:
                continue

            # Regional-specific validation based on historical patterns
            if region == "Africa":
                # Africa historically has had higher child mortality rates
                assert rates.min() >= 0, f"{region}: Negative mortality rate"
                assert rates.max() <= 80, f"{region}: Unrealistically high mortality rate: {rates.max()}%"

            elif region == "Europe":
                # Europe should have lower rates, especially in recent decades
                recent_data = region_data[region_data["year"] >= 1950]
                if len(recent_data) > 0:
                    recent_rates = recent_data[rate_col].dropna()
                    if len(recent_rates) > 0:
                        assert recent_rates.max() <= 30, (
                            f"{region}: Unexpectedly high recent mortality rate: {recent_rates.max()}%"
                        )

            elif region == "Asia":
                # Asia has varied significantly but should be reasonable
                assert rates.min() >= 0, f"{region}: Negative mortality rate"
                assert rates.max() <= 70, f"{region}: Unrealistically high mortality rate: {rates.max()}%"

            elif region in ["North America", "Oceania"]:
                # Generally lower mortality rates expected
                recent_data = region_data[region_data["year"] >= 1950]
                if len(recent_data) > 0:
                    recent_rates = recent_data[rate_col].dropna()
                    if len(recent_rates) > 0:
                        assert recent_rates.max() <= 25, (
                            f"{region}: Unexpectedly high recent mortality rate: {recent_rates.max()}%"
                        )

            elif region == "South America":
                # Moderate rates expected
                assert rates.min() >= 0, f"{region}: Negative mortality rate"
                assert rates.max() <= 50, f"{region}: Unrealistically high mortality rate: {rates.max()}%"

            elif region == "World":
                # World average should be reasonable
                assert rates.min() >= 0, f"{region}: Negative mortality rate"
                assert rates.max() <= 60, f"{region}: Unrealistically high world average: {rates.max()}%"

                # World rates should show general declining trend over time (allowing for fluctuations)
                if len(rates) >= 3:
                    # Check that recent decades show improvement compared to early 1900s
                    early_data = region_data[region_data["year"] <= 1920]
                    recent_data = region_data[region_data["year"] >= 1990]

                    if len(early_data) > 0 and len(recent_data) > 0:
                        early_avg = early_data[rate_col].mean()
                        recent_avg = recent_data[rate_col].mean()

                        # Allow for some cases where this might not hold due to data quality/coverage
                        if not (recent_avg < early_avg * 0.8):  # Recent should be at least 20% lower
                            print(
                                f"Warning: {region} mortality rates haven't improved as expected (early: {early_avg:.1f}%, recent: {recent_avg:.1f}%)"
                            )

    # Check that regional rates are generally consistent with World being a weighted average
    # Note: We can't do exact population-weighted averages without population data,
    # but we can do basic reasonableness checks
    for tb, name in [(tb_combined_full, "full"), (tb_combined_sel, "selected")]:
        rate_col = "child_mortality_rate_full" if name == "full" else "child_mortality_rate"

        if rate_col not in tb.columns:
            continue

        # For each year, check that World rate is within reasonable bounds of regional rates
        years_with_world = tb[tb["country"] == "World"]["year"].unique()

        for year in years_with_world:
            year_data = tb[tb["year"] == year]
            world_rate = year_data[year_data["country"] == "World"][rate_col]

            if len(world_rate) == 0:
                continue

            world_rate = world_rate.iloc[0]

            # Get regional rates for the same year
            regional_rates = year_data[
                year_data["country"].isin(["Asia", "Africa", "Europe", "North America", "South America", "Oceania"])
            ][rate_col].dropna()

            if len(regional_rates) >= 2:  # Need at least 2 regions for comparison
                min_regional = regional_rates.min()
                max_regional = regional_rates.max()

                # World rate should generally be between min and max regional rates
                # Allow some tolerance for data quality and missing regions
                tolerance = 5.0  # 5 percentage points

                if not (min_regional - tolerance <= world_rate <= max_regional + tolerance):
                    print(
                        f"Warning: World rate ({world_rate:.1f}%) outside regional bounds ({min_regional:.1f}%-{max_regional:.1f}%) in year {year}"
                    )

    print("Regional data validation completed")
