"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("global_terrorism_index")

    # Read table from meadow dataset.
    tb = ds_meadow.read("global_terrorism_index")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load population
    ds_pop = paths.load_dataset("population")

    # Load deaths data from UN WPP dataset
    ds_deaths = paths.load_dataset("un_wpp")
    tb_deaths = ds_deaths.read("deaths")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS,
        ds_regions=ds_regions,
    )
    tb = geo.add_population_to_table(tb, ds_population=ds_pop)

    # Calculate terrorism deaths per 100,000 people
    tb["terrorism_deaths_per_100k"] = tb["fatalities"] / (tb["population"] / 100000)

    # Filter deaths data for relevant demographics (all sexes, all ages, estimates variant)
    tb_deaths = tb_deaths[
        (tb_deaths["sex"] == "all") & (tb_deaths["age"] == "all") & (tb_deaths["variant"] == "estimates")
    ][["country", "year", "deaths"]]

    # Merge terrorism data with deaths data
    tb = tb.merge(tb_deaths, on=["country", "year"], how="left")

    # Calculate share of deaths from terrorism
    tb["share_of_deaths_from_terrorism"] = (tb["fatalities"] / tb["deaths"]) * 100

    tb = tb.drop(columns=["deaths", "population"])

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
