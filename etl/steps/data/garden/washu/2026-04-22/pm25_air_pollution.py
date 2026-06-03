"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers.geo import REGIONS
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_NEW = [reg for reg in REGIONS.keys()] + ["World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("pm25_air_pollution")

    # Read table from meadow dataset.
    tb = ds_meadow.read("pm25_air_pollution")

    tb_col_rename = {
        "population_weighted_pm2_5__ug_m3": "population_weighted_pm25",
        "geographic_mean_pm2_5__ug_m3": "geographic_mean_pm25",
        "pct_pop__gt__5_ug_m3__pct": "share_exposure_greater_5_ug_m3",
        "pct_pop__gt__10_ug_m3__pct": "share_exposure_greater_10_ug_m3",
        "pct_pop__gt__15_ug_m3__pct": "share_exposure_greater_15_ug_m3",
        "pct_pop__gt__25_ug_m3__pct": "share_exposure_greater_25_ug_m3",
        "pct_pop__gt__30_ug_m3__pct": "share_exposure_greater_30_ug_m3",
        "pct_pop__gt__35_ug_m3__pct": "share_exposure_greater_35_ug_m3",
        "pct_pop__gt__45_ug_m3__pct": "share_exposure_greater_45_ug_m3",
        "pct_pop__gt__50_ug_m3__pct": "share_exposure_greater_50_ug_m3",
        "pct_pop__gt__55_ug_m3__pct": "share_exposure_greater_55_ug_m3",
        "pct_pop__gt__60_ug_m3__pct": "share_exposure_greater_60_ug_m3",
    }

    tb = tb.rename(columns=tb_col_rename)

    # keep only relevant columns
    tb = tb[["country", "year"] + [col for col in tb_col_rename.values()]]

    # calculate shares of population levels under each interim target
    # under 5 ug/m3 WHO AQG
    tb["share_exposure_under_5_ug_m3"] = 100 - tb["share_exposure_greater_5_ug_m3"]

    # between 5 and 10 ug/m3
    tb["share_exposure_under_10_greater_5ug"] = (
        tb["share_exposure_greater_5_ug_m3"] - tb["share_exposure_greater_10_ug_m3"]
    )
    # between 10 and 15 ug/m3
    tb["share_exposure_under_15_ug_m3_greater_10ug"] = (
        tb["share_exposure_greater_10_ug_m3"] - tb["share_exposure_greater_15_ug_m3"]
    )
    # between 15 and 25 ug/m3
    tb["share_exposure_under_25_greater_15ug"] = (
        tb["share_exposure_greater_15_ug_m3"] - tb["share_exposure_greater_25_ug_m3"]
    )
    # between 25 and 35 ug/m3
    tb["share_exposure_under_35_greater_25ug"] = (
        tb["share_exposure_greater_25_ug_m3"] - tb["share_exposure_greater_35_ug_m3"]
    )

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # create regional aggregates for population weighted PM2.5
    tb = paths.regions.add_population(tb)

    tb["population_times_pm25"] = tb["population"] * tb["population_weighted_pm25"]

    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS_NEW,
        aggregations={"population_times_pm25": "sum", "population": "sum"},
    )

    # add population for regions
    tb = paths.regions.add_population(tb=tb, population_col="full_population")
    # get ratio of covered population:
    tb["population_coverage"] = tb["population"] / tb["full_population"]
    # get population weighted PM2.5 for regions:
    tb.loc[tb["country"].isin(REGIONS_NEW), "population_weighted_pm25"] = tb[tb["country"].isin(REGIONS_NEW)].apply(
        lambda row: row["population_times_pm25"] / row["population"] if row["population_coverage"] > 0.8 else None,
        axis=1,
    )

    tb = tb.drop(columns=["population_times_pm25", "population_coverage", "full_population", "population"])

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
