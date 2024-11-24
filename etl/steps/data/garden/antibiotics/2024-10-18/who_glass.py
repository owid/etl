"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("who_glass")
    # Add population dataset.
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb = ds_meadow["who_glass"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Caclulate tests with ast per million
    tb = add_population_to_table(tb, ds_population)
    tb["total_specimen_isolates_with_ast_per_million"] = (
        tb["total_specimen_isolates_with_ast"] / tb["population"] * 1_000_000
    )
    tb = tb.format(["country", "year", "syndrome"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
