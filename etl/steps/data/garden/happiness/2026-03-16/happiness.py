"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [reg for reg in geo.REGIONS if reg != "European Union (27)"] + ["World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    snap = paths.load_snapshot()
    tb = snap.read()

    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")

    # drop unused columns
    tb = tb[["Year", "Rank", "Country name", "Life evaluation (3-year average)"]]
    # rename columns
    tb = tb.rename(
        columns={
            "Year": "year",
            "Rank": "rank",
            "Country name": "country",
            "Life evaluation (3-year average)": "ladder_score",
        }
    )

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # add population weighted averages
    tb = geo.add_population_to_table(tb, ds_population)  # type: ignore

    tb["cantril_times_pop"] = tb["ladder_score"] * tb["population"]
    aggr_score = {"cantril_times_pop": "sum", "population": "sum"}

    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggr_score,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
    )

    tb.loc[tb["country"].isin(REGIONS), "ladder_score"] = tb["cantril_times_pop"] / tb["population"]

    tb = tb.drop(columns=["cantril_times_pop", "population"])
    tb = tb.rename(columns={"ladder_score": "cantril_ladder_score"})

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save garden dataset.
    ds_garden.save()
