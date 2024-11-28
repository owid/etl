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
    ds_meadow = paths.load_dataset("antimicrobial_usage")

    # Read table from meadow dataset.
    tb_class = ds_meadow["class"].reset_index()
    tb_aware = ds_meadow["aware"].reset_index()
    #
    # Process data.
    #
    tb_class = geo.harmonize_countries(df=tb_class, countries_file=paths.country_mapping_path)
    tb_aware = geo.harmonize_countries(df=tb_aware, countries_file=paths.country_mapping_path)

    # Drop columns that are not needed in the garden dataset.
    tb_class = tb_class.drop(
        columns=["whoregioncode", "whoregionname", "countryiso3", "incomeworldbankjune", "atc4", "notes"]
    )
    tb_aware = tb_aware.drop(columns=["whoregioncode", "whoregionname", "incomeworldbankjune", "aware", "notes"])

    tb_class = tb_class.format(["country", "year", "antimicrobialclass", "atc4name", "routeofadministration"])
    tb_aware = tb_aware.format(["country", "year", "awarelabel"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_class, tb_aware], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
