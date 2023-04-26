"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("wildlife_seizures.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("wildlife_seizures")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wildlife_seizures"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    df = df.drop(["iso3_code", "region", "subregion", "indicator"], axis=1)

    #
    # Process data.
    #
    log.info("wildlife_seizures.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    df = df.pivot(index=["country", "year"], columns=["taxonomic_group", "unit_of_measurement"], values="value")
    df = df.reset_index()
    df.columns = [" ".join(col).strip() for col in df.columns.values]
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wildlife_seizures.end")
