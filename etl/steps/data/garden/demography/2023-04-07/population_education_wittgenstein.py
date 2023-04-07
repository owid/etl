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
    log.info("population_education_wittgenstein.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("population_education_wittgenstein")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["population_education_wittgenstein"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)
    df = df.rename(columns={"area": "country"})
    df["population"] = df["population"] * 1000
    #
    # Process data.
    #
    log.info("population_education_wittgenstein.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    df = df.set_index(["country", "year", "education"], verify_integrity=True)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("population_education_wittgenstein.end")
