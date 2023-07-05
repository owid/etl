"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("oil_spills.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("oil_spills")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["oil_spills"]
    # Convert the table 'tb_meadow' into a pandas DataFrame.
    df = pd.DataFrame(tb_meadow)

    # Log that the process of harmonizing countries has started.
    log.info("oil_spills.harmonize_countries")
    # Call a function to harmonize country names in the DataFrame.
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Group the data by decade for 'world' country only
    for column in ["bel_700t", "ab_700t", "oil_spilled"]:
        mask = df["country"] == "World"  # Filter for 'world' country
        df.loc[mask, "decadal_" + str(column)] = (
            df.loc[mask, column].groupby(df.loc[mask, "year"] // 10 * 10).transform("mean")
        )
        # set NaN everywhere except start of a decade
        df.loc[mask, "decadal_" + str(column)] = df.loc[mask, "decadal_" + str(column)].where(
            df.loc[mask, "year"] % 10 == 0, np.nan
        )
    # Replace any '__' in column names with a space (done because of double _ in some variable names)
    newnames = [name.replace("__", "_") for name in df.columns]
    df.columns = newnames

    # Convert the 'country' column to a string type.
    df["country"] = df["country"].astype(str)

    # Append the 'year' to the 'country' column's entries where 'country' equals 'La Coruna, Spain'.
    df.loc[df["country"] == "La Coruna, Spain", "country"] = (
        df.loc[df["country"] == "La Coruna, Spain", "country"]
        + ", "
        + df.loc[df["country"] == "La Coruna, Spain", "year"].astype(str)
    )
    for col in df.columns:
        if col not in ["country", "year"]:
            df[col] = df[col].round(0)

    df["country"] = df["country"].str.replace(",", "")  # to avoid double ,,
    df.set_index(["country", "year"], verify_integrity=True, inplace=True)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    tb_garden.metadata = TableMeta(short_name=paths.short_name, primary_key=list(df.index.names))

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("oil_spills.end")
