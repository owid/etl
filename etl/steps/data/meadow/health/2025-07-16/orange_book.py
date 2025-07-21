"""Load a snapshot and create a meadow dataset."""

import zipfile

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# 26559 rows × 14 columns
# 84 x 14 columns


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("orange_book.zip")

    with zipfile.ZipFile(snap.path, "r") as z:
        with z.open("products.txt") as f:
            df = pd.read_csv(
                f,
                sep="~",
            )

    snap_meta = snap.to_table_metadata()

    tb = Table(df, metadata=snap_meta, short_name="orange_book")

    # Remove products with higher product number but same Application number and same ingredients as a product with lower product number.
    tb = tb.sort_values("Product_No")
    tb[~tb.duplicated(subset=["Appl_No", "Ingredient"], keep="first")]

    tb = tb.sort_values("Appl_No")

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["appl_no", "product_no", "ingredient"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
