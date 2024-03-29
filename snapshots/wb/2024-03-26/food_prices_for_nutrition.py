"""Script to create a snapshot of dataset 'Food Prices for Nutrition'."""

from pathlib import Path

import click
import pandas as pd
import wbgapi as wb
from owid.datautils.dataframes import map_series
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Dataset id of the World Bank's Food Prices for Nutrition dataset.
WB_FOOD_PRICES_DATASET_ID = 88


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/food_prices_for_nutrition.csv")

    # List all variable ids and titles in the food prices dataset
    variables = wb.series.info(db=WB_FOOD_PRICES_DATASET_ID)

    # Load data for each variable.
    # Get data for all variables one by one.
    data = []
    # Note: This takes a few minutes and could possibly be parallelized.
    for variable in tqdm(variables.items):
        # Load data for current variable and add it to the list of all dataframes.
        variable_df = wb.data.DataFrame(db=WB_FOOD_PRICES_DATASET_ID, series=variable["id"])
        variable_df["id"] = variable["id"]
        data.append(variable_df)

        # Note: In theory, metadata can also be fetched with the API, but if fails with JSONDecodeError.
        # variable_metadata = wb.series.metadata.get(variable["id"])

    # Combine all dataframes into one.
    df = pd.concat(data)

    # Add variable titles to the datafrme as a new column.
    df["variable_title"] = map_series(
        series=df["id"],
        mapping={variable["id"]: variable["value"] for variable in variables.items},
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )

    # Add data to snapshot and upload.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
