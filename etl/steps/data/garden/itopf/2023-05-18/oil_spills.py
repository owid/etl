"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("oil_spills.start")
    print("hi")

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

    # Create an empty DataFrame to store the average values per decade.
    decadal_averages_df = pd.DataFrame()

    # Iterate over the columns of the original DataFrame.
    for column in df.columns:
        # Select specific columns of interest.
        if column in ["bel_700t", "ab_700t", "oil_spilled"]:
            # Group the data by decade (dividing the year by 10, rounding down and multiplying by 10 again),
            # and calculate the mean for the specified column. Drop NaN results.
            decadal_averages = df.groupby(df["year"] // 10 * 10)[column].mean().dropna()

            # Construct new column names for the decadal averages.
            decadal_column = "decadal_" + str(column)

            # Add the calculated decadal averages to the new DataFrame, rounding and converting to integers.
            decadal_averages_df[decadal_column] = np.round(decadal_averages).astype(int)

    # Merge the original DataFrame with the DataFrame containing the decadal averages,
    # using 'year' as the key and keeping all records from both DataFrames ('outer' join).
    df_decadal = pd.merge(df, decadal_averages_df, on="year", how="outer", validate="many_to_one")

    # Reset the DataFrame's index to default.
    df_decadal.reset_index(inplace=True)

    # Replace any '__' in column names with a space (done because of double _ in some variable names)
    newnames = [name.replace("__", " ") for name in df_decadal.columns]
    df_decadal.columns = newnames

    # Convert the 'country' column to a string type.
    df_decadal["country"] = df_decadal["country"].astype(str)

    # Append the 'year' to the 'country' column's entries where 'country' equals 'La Coruna, Spain'.
    df_decadal.loc[df_decadal["country"] == "La Coruna, Spain", "country"] = (
        df_decadal.loc[df_decadal["country"] == "La Coruna, Spain", "country"]
        + ", "
        + df_decadal.loc[df_decadal["country"] == "La Coruna, Spain", "year"].astype(str)
    )

    # Create a new table with the processed data.
    tb_garden = Table(df_decadal, short_name="oil_spills")

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("oil_spills.end")
