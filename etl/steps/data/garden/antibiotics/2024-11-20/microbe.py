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
    ds_meadow = paths.load_dataset("microbe")

    # Read table from meadow dataset.
    tb = ds_meadow.read("microbe")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.drop(columns=["age", "sex", "measure", "metric", "counterfactual", "infectious_syndrome"])
    # Create a table where the pathogen is the entity
    tb_pathogen = tb.drop(columns=["country", "pathogen_type"]).rename(columns={"pathogen": "country"})
    tb = tb.format(["country", "year", "pathogen_type", "pathogen"])
    tb_pathogen = tb_pathogen.format(["country", "year"], short_name="pathogen_entity")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_pathogen], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
