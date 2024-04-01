"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("mueller_et_al_2012")
    tb = ds_meadow["mueller_et_al_2012"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    # Add variable metadata.
    for column in tb.columns:
        item = column.split("_")[0]
        tb[column].metadata.title = f"Attainable yield for {item}"
        tb[column].metadata.units = "tonnes per hectare"
        tb[column].metadata.units = "t/ha"

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
