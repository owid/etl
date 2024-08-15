"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
from owid.catalog import Dataset, Table
from owid.catalog.processing import concat
from shared import add_population_2022, add_population_daily, add_regions

from etl.data_helpers import geo
from etl.data_helpers.misc import expand_time_column, interpolate_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccinations_global")
    ds_regions = paths.load_dataset("regions")
    ds_income = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["vaccinations_global"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Dtypes
    tb = tb.astype(
        {
            "country": "string",
            "total_vaccinations": float,
            "people_vaccinated": float,
            "people_fully_vaccinated": float,
            "total_boosters": float,
        }
    )

    # Add daily vaccinations
    tb = add_daily_vaccinations(tb)

    # Add interpolation
    tb = add_interpolated_indicators(tb)

    # Add smoothed indicators
    tb = add_smoothed_indicators(tb)

    # Add population
    tb = add_population_2022(
        tb=tb,
        ds_population=ds_population,
    )

    # Aggregate
    tb = add_regional_aggregates(tb, ds_regions, ds_income)

    # Per capita
    tb = add_per_capita(tb)

    # Add unvaccinated
    tb = add_people_unvaccinated(tb, ds_population)

    # Share of boosters
    tb = add_booster_share(tb)

    # Sanity checks
    sanity_checks(tb)

    # Drop columns
    tb = tb.drop(
        columns=[
            "new_vaccinations_interpolated",
            "new_people_vaccinated_interpolated",
        ]
    )

    # Rename
    tb = tb.rename(
        columns={
            "new_vaccinations": "daily_vaccinations",
            "new_vaccinations_smoothed": "daily_vaccinations_smoothed",
            "new_vaccinations_smoothed_per_million": "daily_vaccinations_smoothed_per_million",
            "new_people_vaccinated_smoothed": "daily_people_vaccinated_smoothed",
            "new_people_vaccinated_smoothed_per_hundred": "daily_people_vaccinated_smoothed_per_hundred",
        }
    )

    # Format
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_daily_vaccinations(tb: Table) -> Table:
    """Get daily vaccinations."""
    tb = tb.sort_values(by=["country", "date"])
    # Estimate daily difference
    tb["new_vaccinations"] = tb.groupby("country").total_vaccinations.diff()
    # Set to NaN if day-diff is not 1 day (i.e. make sure `new_vaccinations` only contains daily values)
    tb.loc[tb.date.diff().dt.days > 1, "new_vaccinations"] = np.nan
    return tb


def add_interpolated_indicators(tb: Table) -> Table:
    """Add linearly interpolated indicators.

    New columns:
    - new_vaccinations_interpolated
    - new_people_vaccinated_interpolated
    """

    # Interpolate
    tb_int = tb.loc[:, ["country", "date", "total_vaccinations", "people_vaccinated"]]
    tb_int = interpolate_table(
        tb_int,
        "country",
        "date",
        time_mode="full_range_entity",
    )

    # Estimate difference
    cols = ["total_vaccinations", "people_vaccinated"]
    tb_int[cols] = tb_int.groupby("country")[cols].diff()

    # Rename
    tb_int = tb_int.rename(
        columns={
            "total_vaccinations": "new_vaccinations_interpolated",
            "people_vaccinated": "new_people_vaccinated_interpolated",
        }
    )

    # Add to main table
    tb = tb.merge(tb_int, on=["country", "date"], how="outer")

    return tb


def add_smoothed_indicators(tb: Table) -> Table:
    """Smooth values."""
    columns = [
        "new_vaccinations_interpolated",
        "new_people_vaccinated_interpolated",
    ]

    tb[["new_vaccinations_smoothed", "new_people_vaccinated_smoothed"]] = (
        tb.groupby("country")[columns].rolling(window=7, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    return tb


def add_regional_aggregates(tb: Table, ds_regions: Dataset, ds_income: Dataset) -> Table:
    tb_agg = _prepare_table_for_aggregates(tb)
    tb_agg.loc[:, "new_vaccinations"] = tb_agg.loc[:, "new_vaccinations_interpolated"].copy()
    tb_agg = add_regions(tb_agg, ds_regions, ds_income, keep_only_regions=True)
    tb = concat([tb, tb_agg], ignore_index=True)
    return tb


def _prepare_table_for_aggregates(tb: Table) -> Table:
    """Prepare table for region-aggregate values.

    Often, values for certain countries are missing. This can lead to very large under-estimates regional values. To mitigate this, we combine zero-filling with interpolation and other techniques.
    """
    tb_agg = expand_time_column(tb, "country", "date", "full_range")
    cols_index = ["country", "date"]
    # cumulative metrics: Interpolate, forward filling (for latest) + zero-filling (for remaining NaNs, likely at start)
    cols_ffill = [
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "total_boosters",
    ]
    tb_agg_1 = interpolate_table(
        df=tb_agg[cols_ffill + cols_index],
        entity_col="country",
        time_col="date",
        time_mode="none",
    )

    tb_agg_1.loc[:, cols_ffill] = tb_agg_1.groupby("country")[cols_ffill].ffill().fillna(0)

    # daily metrics: zero fill
    cols_0fill = [
        "new_vaccinations",
        "new_vaccinations_interpolated",
        "new_vaccinations_smoothed",
        "new_people_vaccinated_interpolated",
        "new_people_vaccinated_smoothed",
    ]
    tb_agg_2 = tb_agg[cols_0fill + cols_index]
    tb_agg_2.loc[:, cols_0fill] = tb_agg_2.groupby("country")[cols_0fill].fillna(0)

    # population
    tb_agg_3 = tb_agg[["population"] + cols_index]
    tb_agg_3.loc[:, "population"] = tb_agg_3.groupby("country")["population"].ffill().bfill()

    # Merge
    tb_agg = tb_agg_1.merge(tb_agg_2, on=["country", "date"], validate="one_to_one")
    tb_agg = tb_agg.merge(tb_agg_3, on=["country", "date"], validate="one_to_one")

    return cast(Table, tb_agg)


def add_per_capita(tb: Table) -> Table:
    """Add per-capita metrics"""
    per_100 = [
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "total_boosters",
        "new_people_vaccinated_smoothed",
    ]
    per_1m = ["new_vaccinations_smoothed"]
    for col in per_100:
        tb[f"{col}_per_hundred"] = tb[col] * 100 / tb["population"]
    for col in per_1m:
        tb[f"{col}_per_million"] = tb[col] * 1_000_000 / tb["population"]

    # drop population
    tb = tb.drop(columns="population")

    return tb


def add_people_unvaccinated(tb: Table, ds_population: Dataset) -> Table:
    """Get un-vaccinated people."""
    tb = add_population_daily(tb, ds_population)
    tb["people_unvaccinated"] = tb["population"] - tb["people_vaccinated"]
    tb.loc[tb["people_unvaccinated"] < 0, "people_unvaccinated"] = 0
    tb["people_unvaccinated"].m.presentation.attribution = None
    return tb


def sanity_checks(tb: Table) -> None:
    """Minor sanity checks on indicator figures."""
    # Config
    skip_countries = ["Pitcairn"]
    # Sanity checks
    df_to_check = tb.loc[~tb["country"].isin(skip_countries)]
    if not (df_to_check["total_vaccinations"].dropna() >= 0).all():
        raise ValueError("Negative values found! Check values in `total_vaccinations`.")
    if not (df_to_check["new_vaccinations_smoothed"].dropna() >= 0).all():
        raise ValueError("Negative values found! Check values in `new_vaccinations_smoothed`.")
    if not (msk := (x := df_to_check["new_vaccinations_smoothed_per_million"].dropna()) <= 120000).all():
        example = df_to_check.loc[x[~msk].index, ["date", "location", "new_vaccinations_smoothed_per_million"]]
        raise ValueError(f"Huge values found! Check values in `new_vaccinations_smoothed_per_million`: \n{example}")


def add_booster_share(tb: Table) -> Table:
    shape_before = tb.shape
    global_boosters = tb.loc[tb["country"] == "World"][
        ["country", "date", "total_vaccinations", "total_boosters"]
    ].sort_values("date")
    global_boosters[["total_vaccinations", "total_boosters"]] = global_boosters[
        ["total_vaccinations", "total_boosters"]
    ].astype(float)
    global_boosters["share_of_boosters"] = (
        (
            (global_boosters["total_boosters"] - global_boosters["total_boosters"].shift(1))
            / (global_boosters["total_vaccinations"] - global_boosters["total_vaccinations"].shift(1))
        )
        .rolling(14)
        .mean()
        .round(4)
    )
    global_boosters = global_boosters.drop(columns=["total_vaccinations", "total_boosters"])
    tb = tb.merge(global_boosters, how="left", on=["country", "date"], validate="one_to_one")
    assert (
        tb.shape[0] == shape_before[0] and tb.shape[1] == shape_before[1] + 1
    ), "Adding share_of_boosters has changed the shape of the dataframe in an unintended way!"
    return tb
