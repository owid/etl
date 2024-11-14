"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("co2_air_transport.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    columns_to_use = [
        "Reference area",
        "Frequency of observation",
        "EMISSIONS_SOURCE",
        "Flight type",
        "TIME_PERIOD",
        "OBS_VALUE",
    ]
    tb = tb[columns_to_use]

    # Convert the 'year' column to datetime
    tb["TIME_PERIOD"] = pd.to_datetime(tb["TIME_PERIOD"], format="mixed")

    # Extract the month and year from 'year' column and create new columns
    tb["month"] = tb["TIME_PERIOD"].dt.month
    tb["year"] = tb["TIME_PERIOD"].dt.year
    tb = tb.drop(["TIME_PERIOD"], axis=1)

    tb = tb.rename(columns={"Reference area": "country", "OBS_VALUE": "value"})
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "month", "flight_type", "frequency_of_observation", "emissions_source"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
