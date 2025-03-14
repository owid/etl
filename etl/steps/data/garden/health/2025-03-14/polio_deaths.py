"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_phr = paths.load_dataset("polio_deaths", namespace="public_health_reports")
    ds_cdc = paths.load_dataset("polio_deaths", namespace="cdc")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_phr = ds_phr.read("polio_deaths")
    tb_cdc = ds_cdc.read("polio_deaths")
    tb_pop = ds_population.read("population", reset_metadata="keep_origins")

    tb = pr.concat([tb_phr, tb_cdc], axis=0)
    tb = tb.drop(columns=["source"])

    tb = pr.merge(tb, tb_pop, on=["country", "year"], how="left", indicator=True)
    tb["death_rate"] = tb["deaths"] / tb["population"] * 100000
    tb = tb.drop(columns=["population", "_merge", "source", "world_pop_share"])
    #
    # Process data.

    # Improve table format.
    tb = tb.format(["country", "year"], short_name="polio_deaths")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
