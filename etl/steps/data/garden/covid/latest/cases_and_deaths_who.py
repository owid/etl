import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data from github.
    #
    url = paths.load_etag_url()

    df = (
        pd.read_csv(url)
        .rename(columns={"Year": "year", "Country": "country"})
        .set_index(["year", "country"])
        .dropna(axis=0, how="all")
    )

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()
