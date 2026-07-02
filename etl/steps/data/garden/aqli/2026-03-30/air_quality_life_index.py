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
    ds_meadow = paths.load_dataset("air_quality_life_index")

    # Read table from meadow dataset.
    tb = ds_meadow.read("air_quality_life_index")

    tb = tb[
        [
            "country",
            "year",
            "whostandard",
            "natstandard",
            "pm",
            "llpp_who",
            "llpp_nat",
        ]
    ]

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # create regional aggregates for population weighted PM2.5
    tb = paths.regions.add_population(tb)

    tb["population_times_pm25"] = tb["population"] * tb["pm"]

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
    tb.loc[tb["country"].isin(REGIONS_NEW), "pm"] = tb[tb["country"].isin(REGIONS_NEW)].apply(
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
