"""Load a snapshot and create a meadow dataset."""

import json
import zipfile

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def results_to_tables(data, snap):
    """Converts results from the JSON data into a structured table format.
    Uses one table for submission data and another for product data.
    """
    results = data["results"]
    submissions = []
    products = []
    open_fda = []

    for res in results:
        # Extract general information:
        keys = res.keys()
        if "application_number" not in keys or "sponsor_name" not in keys:
            raise ValueError("Expected keys 'application_number' and 'sponsor_name' in each result.")

        id_entry = {
            "application_number": res["application_number"],
            "sponsor_name": res["sponsor_name"],
        }

        # Extract submission information:
        if "submissions" in keys:
            res_submission = res["submissions"]
            for sub in res_submission:
                sub.update(id_entry)
            submissions.extend(res_submission)

        # Extract open_fda information:
        if "openfda" in keys:
            res_open_fda = res["openfda"]
            res_open_fda.update(id_entry)
            open_fda.append(res_open_fda)

        # Extract product information:
        if "products" in keys:
            res_products = res["products"]
            for prod in res_products:
                prod.update(id_entry)
                prod.update({"active_ingredients": ", ".join([entry["name"] for entry in prod["active_ingredients"]])})

            products.extend(res_products)

    return products, submissions, open_fda

    # Total missing submissions: 2706, 9.53% of total.
    # Total missing products: 362, 1.28% of total.
    # Total missing fda information: 16087, 56.68% of total.

    # snap_meta = snap.to_table_metadata()

    # tb_sub = Table(submissions, metadata=snap_meta, short_name="drugs_fda_submissions")
    # tb_products = Table(products, metadata=snap_meta, short_name="drugs_fda_products")
    # tb_open_fda = Table(open_fda, metadata=snap_meta, short_name="drugs_open_fda")
    # return tb_sub, tb_products, tb_open_fda


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("drugs_at_fda_json.zip")

    # Load data from snapshot.
    with zipfile.ZipFile(snap.path, "r") as z:
        with z.open("drug-drugsfda-0001-of-0001.json") as f:
            data = json.load(f)

    products, submissions, open_fda = results_to_tables(data, snap)
    products = pd.DataFrame(products)
    submissions = pd.DataFrame(submissions)
    open_fda = pd.DataFrame(open_fda)

    submissions = submissions[submissions["submission_status"] == "AP"]

    submissions["approval_year"] = submissions["submission_status_date"].apply(lambda x: str(x)[:4])

    submissions = submissions[
        [
            "application_number",
            "submission_type",
            "submission_status_date",
            "approval_year",
        ]
    ]

    submissions = pd.merge(
        submissions,
        products,
        on="application_number",
        how="left",
        suffixes=("_submission", "_product"),
    )

    df_anxiety_meds = pd.read_csv("/Users/tunaacisu/Data/FDA_drugs/Anxiety drugs list - Final.csv")
    df_anxiety_meds = df_anxiety_meds.dropna(subset=["Drug_name"])

    # get minimum year where the drug was approved
    df_anxiety_meds["min_approval_year"] = None
    df_anxiety_meds["notes"] = None

    for _, row in df_anxiety_meds.iterrows():
        # if the drug name is in the anxiety meds list, set the Anxiety_Med column to True
        drug_name = row["Drug_name"]
        print(drug_name)
        print(f"Finding {drug_name} minimum approval year...")

        mask = (
            submissions["active_ingredients"]
            .astype(str)  # coerce non-strings
            .str.lower()
            .str.contains(drug_name.lower(), na=False)
        )

        this_drug = submissions[mask]
        print(f"Found {len(this_drug)} products for {drug_name}.")
        if this_drug.empty:
            df_anxiety_meds.loc[_, "min_approval_year"] = None
            df_anxiety_meds.loc[_, "notes"] = "No products found in the FDA database."
            continue
        min_approval_idx = this_drug["approval_year"].idxmin()

        min_approval_year = this_drug.loc[min_approval_idx, "approval_year"]
        min_approval_brand_name = this_drug.loc[min_approval_idx, "brand_name"]
        min_approval_marketing_status = this_drug.loc[min_approval_idx, "marketing_status"]
        min_approval_sponsor_name = this_drug.loc[min_approval_idx, "sponsor_name"]
        min_approval_appli_no = this_drug.loc[min_approval_idx, "application_number"]

        print(f"Minimum approval year for {drug_name} is {min_approval_year}.")
        df_anxiety_meds.loc[_, "min_approval_year"] = min_approval_year
        df_anxiety_meds.loc[_, "notes"] = (
            f"Brand name: {min_approval_brand_name}, \nMarketing status: {min_approval_marketing_status}, \nSponsor name: {min_approval_sponsor_name}, \nApplication number: {min_approval_appli_no}"
        )

    df_anxiety_meds.to_csv("/Users/tunaacisu/Data/FDA_drugs/Anxiety_drugs_approval_years.csv", index=False)
