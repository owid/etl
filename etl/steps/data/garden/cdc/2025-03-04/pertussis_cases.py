"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("pertussis_cases")
    ds_population = paths.load_dataset("population")
    tb_pop = ds_population.read("population", reset_metadata="keep_origins")
    # Read table from meadow dataset.
    tb = ds_meadow.read("pertussis_cases")

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = pr.merge(
        tb,
        tb_pop,
        on=["country", "year"],
        how="left",
    )
    tb["case_rate"] = tb["cases"] / tb["population"] * 100000

    tb = tb.drop(columns=["population", "source", "world_pop_share"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
