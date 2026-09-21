"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Labels of the three scenarios in the original data, and names of the output columns.
SCENARIOS = {
    "Business As Usual": "cropland_business_as_usual",
    "Towards Sustainability": "cropland_towards_sustainability",
    "Stratified Societies": "cropland_stratified_societies",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("future_of_food_and_agriculture_arable_land")

    # Read table from meadow dataset.
    tb = ds_meadow.read("future_of_food_and_agriculture_arable_land")

    #
    # Process data.
    #
    # Sanity checks.
    assert set(tb["scenario"]) == set(SCENARIOS) | {"Historical", "Base year"}, "Scenarios have changed."
    assert (tb["arable_land"] > 0).all(), "Arable land has non-positive values."

    # Remove the historical value (1970), which is already covered by the observed FAOSTAT data.
    tb = tb[tb["scenario"] != "Historical"].reset_index(drop=True)

    # The base year (2012) is shared by the three scenarios; assign it to each of them.
    tb_base = tb[tb["scenario"] == "Base year"]
    tb = pr.concat(
        [tb[tb["scenario"] == scenario] for scenario in SCENARIOS]
        + [tb_base.assign(**{"scenario": scenario}) for scenario in SCENARIOS],
        ignore_index=True,
    )

    # Create a column of arable land (which, following FAOSTAT terminology, is cropland) for each scenario.
    tb = tb.pivot(index=["year"], columns="scenario", values="arable_land", join_column_levels_with="_")
    tb = tb.rename(columns=SCENARIOS, errors="raise")

    # Convert from million hectares to hectares.
    for column in SCENARIOS.values():
        tb[column] *= 1e6

    # The data corresponds to world totals.
    tb["country"] = "World"

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
