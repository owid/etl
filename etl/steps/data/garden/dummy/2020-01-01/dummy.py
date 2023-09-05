"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("dummy")
    tb = ds_meadow["dummy"]
    tb = tb.reset_index()

    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["dummy"].reset_index()
    tb_population = ds_population["population"].reset_index()

    # tb = tb.merge(tb_population["population"], on=["country", "year"])

    tb["dummy_variable"] = tb["dummy_variable"] / tb_population["population"]

    tb["c"] = tb["dummy_variable"] + tb["yummy_variable"] + 5

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
