"""Load a snapshot and create a meadow dataset."""

import zipfile

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# 26559 rows × 14 columns
# 84 x 14 columns


def orange_book_approval_year(approval_date):
    # example approval year: Jun 27, 2025
    if approval_date == "Approved Prior to Jan 1, 1982":
        return 1900  # This is a placeholder year for drugs approved before the Orange Book started tracking approvals.
    else:
        return pd.to_datetime(approval_date, format="%b %d, %Y", errors="coerce").year


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

    df_anxiety_meds = pd.read_csv("/Users/tunaacisu/Data/FDA_drugs/Anxiety drugs list - Final.csv")

    # drug_names = df_anxiety_meds["Drug_name"].unique()

    # get minimum year where the drug was approved
    tb["approval_year"] = tb["Approval_Date"].apply(orange_book_approval_year)
    df_anxiety_meds["min_approval_year"] = None

    for _, row in df_anxiety_meds.iterrows():
        # if the drug name is in the anxiety meds list, set the Anxiety_Med column to True
        drug_name = row["Drug_name"]
        print(f"Finding {drug_name} minimum approval year...")
        this_drug = tb[tb.apply(lambda x: drug_name.lower() in x["Ingredient"].lower(), axis=1)]
        print(f"Found {len(this_drug)} products for {drug_name}.")
        min_approval_year = this_drug["approval_year"].min()
        print(f"Minimum approval year for {drug_name} is {min_approval_year}.")
        df_anxiety_meds.loc[_, "min_approval_year"] = min_approval_year

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
