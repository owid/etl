"""Load a meadow dataset and create a garden dataset."""

import re

from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("urban_agglomerations_definition")

    # Read table from meadow dataset.
    tb = ds_meadow["urban_agglomerations_definition"].reset_index()

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "urban_agglomerations_shared.countries.json"
    tb = geo.harmonize_countries(df=tb, countries_file=country_mapping_path)

    # Copy the definition column first to keep metadata
    tb["minimum_inhabitants"] = tb["definition"].copy()
    # Apply the accurate function to the 'definition' column
    tb["minimum_inhabitants"] = tb["definition"].apply(extract_min_inhabitants_accurate)
    # Drop unnecessary columns
    tb = tb.drop(columns=["definition", "sources"])

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


def extract_min_inhabitants_accurate(definition):
    # Regular expression to find a number possibly with commas, followed by the word "inhabitants"
    match = re.search(r"(\b\d{1,3}(,\d{3})*\b)(?=\s*inhabitants)", definition, re.IGNORECASE)
    if match:
        return match.group(1).replace(",", "")  # Return the number found, removing commas
    return None
