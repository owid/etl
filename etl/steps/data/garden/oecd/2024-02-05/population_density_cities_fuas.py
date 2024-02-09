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
    ds_meadow = paths.load_dataset("population_density_cities_fuas")

    # Read table from meadow dataset.
    tb = ds_meadow["population_density_cities_fuas"].reset_index()

    #
    # Process data.
    #
    cols_to_select = ["reference_area", "year", "value"]
    tb = tb[cols_to_select]
    tb = tb.rename(columns={"value": "population_density"})
    tb = tb.set_index(["reference_area", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
