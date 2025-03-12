"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_phr = paths.load_dataset("polio_cases", namespace="public_health_reports")
    ds_census = paths.load_dataset("polio_cases", namespace="us_census_bureau")
    ds_cdc = paths.load_dataset("polio_cases", namespace="cdc")
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb_phr = ds_phr.read("polio_cases")
    tb_census = ds_census.read("polio_cases")
    tb_cdc = ds_cdc.read("polio_cases")
    tb_pop = ds_population.read("population")

    tb = pr.concat([tb_phr, tb_census, tb_cdc], axis=0)
    tb = tb.drop(columns=["source"])

    tb = pr.merge(tb, tb_pop, on=["country", "year"], how="left", indicator=True)
    tb["case_rate"] = tb["cases"] / tb["population"] * 100000
    tb = tb.drop(columns=["population", "_merge", "source", "world_pop_share"])

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
