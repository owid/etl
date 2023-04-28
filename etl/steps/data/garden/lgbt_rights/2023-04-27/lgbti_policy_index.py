"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference: Dataset = paths.load_dependency("reference")
    tb_countries_regions = ds_reference["countries_regions"]

    return tb_countries_regions


def load_population() -> Table:
    """Load population table from population OMM dataset."""
    ds_indicators: Dataset = paths.load_dependency(channel="garden", namespace="demography", short_name="population")
    tb_population = ds_indicators["population"]

    return tb_population


def run(dest_dir: str) -> None:
    log.info("lgbti_policy_index.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("lgbti_policy_index")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["lgbti_policy_index"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index(drop=False)

    #
    # Process data.
    #
    log.info("lgbti_policy_index.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(df)
    tb_garden.metadata.short_name = "lgbti_policy_index"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # For now the variable descriptions are stored as a list of strings, this transforms them into a single string
    tb_garden = ds_garden["lgbti_policy_index"]
    for col in tb_garden.columns:
        if isinstance(tb_garden[col].metadata.description, list):
            tb_garden[col].metadata.description = "\n".join(tb_garden[col].metadata.description)
    ds_garden.add(tb_garden)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("lgbti_policy_index.end")
