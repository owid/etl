"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("education_opri.zip")

    with snap.extracted() as archive:
        national_df = archive.read("OPRI_DATA_NATIONAL.csv", low_memory=False)
        regional_df = archive.read("OPRI_DATA_REGIONAL.csv", low_memory=False)
        label_df = archive.read("OPRI_LABEL.csv", low_memory=False)

    #
    # Process data.
    #
    # Rename columns with regions and countries for the purpose of merging the dataframes later on
    national_df.columns = national_df.columns.str.lower()

    rename_dict = {"region_id": "country", "country_id": "country"}
    regional_df.rename(columns=rename_dict, inplace=True)
    national_df.rename(columns=rename_dict, inplace=True)

    # Concatenate and merge dataframes with regional and national data
    combined_df = pr.concat([regional_df, national_df], axis=0)
    label_df.columns = label_df.columns.str.lower()

    # Add indicator label column that provides a better description of the indicator
    tb = pr.merge(combined_df, label_df, on="indicator_id", how="left")

    tb = tb.format(["country", "year", "indicator_id"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
