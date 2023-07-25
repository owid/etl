import datetime as dt

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset


def run(dest_dir: str, paths: PathFinder) -> None:
    #
    # Load data from github.
    #
    url = paths.load_etag_url()

    df = pd.read_csv(url).rename(columns={"Year": "year", "Country": "country"})

    # Harmonize country names.
    df.country = df.country.replace(
        {
            "Faeroe Islands": "Faroe Islands",
            "Timor": "East Timor",
        }
    )

    df = df.set_index(["year", "country"]).dropna(axis=0, how="all")

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

    ds_garden.metadata.sources[0].date_accessed = str(dt.date.today())

    # Save changes in the new garden dataset.
    ds_garden.save()
