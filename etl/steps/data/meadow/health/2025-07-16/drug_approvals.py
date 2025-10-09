"""Load a snapshot and create a meadow dataset."""

import json
import time
import zipfile

import pandas as pd
import requests

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


def join_list_in_column(df, column_name, separator=", "):
    """Join list elements in a specified column of a DataFrame into a single string."""
    df[column_name] = df[column_name].apply(lambda x: separator.join(x) if isinstance(x, list) else x)
    return df


ANXIETY_KEYWORDS = ["anxiety", "panic", "GAD"]


def no_anxiety_keywords(text):
    """return True if none of the anxiety keywords are in the text (case sensitive)"""
    if not isinstance(text, str):
        return True
    return all(keyword not in text for keyword in ANXIETY_KEYWORDS)


def get_fda_labels_by_ingredient(active_ingredient, limit=50):
    """
    Fetch all FDA label entries for a given active ingredient from openFDA.
    Returns a pandas DataFrame containing key fields (indications, brand_name, etc.).
    """

    base_url = "https://api.fda.gov/drug/label.json"
    query = f'search=openfda.substance_name:"{active_ingredient}"'
    skip = 0
    all_results = []

    while True:
        url = f"{base_url}?{query}&limit={limit}&skip={skip}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            return pd.DataFrame()  # Return empty DataFrame on error

        data = response.json()
        total_hits = data["meta"]["results"]["total"]
        print(f"Total hits for {active_ingredient}: {total_hits}. Fetching records {skip} to {skip + limit}...")

        results = results_to_dict(data.get("results"))
        all_results.extend(results)

        if not results:
            break

        skip += limit
        if data["meta"]["results"]["total"] < skip:
            break

    df = pd.DataFrame(all_results)

    # join list elements: indications_and_usage, brand name, generic name, application_number, product_ndc, rxcui, spl_id, spl_set_id, unii
    df = join_list_in_column(df, "indications_and_usage", separator=", ")
    df = join_list_in_column(df, "brand_name", separator=", ")
    df = join_list_in_column(df, "generic_name", separator=", ")
    df = join_list_in_column(df, "application_number", separator=", ")
    df = join_list_in_column(df, "product_ndc", separator=", ")
    df = join_list_in_column(df, "rxcui", separator=", ")
    df = join_list_in_column(df, "spl_id", separator=", ")
    df = join_list_in_column(df, "spl_set_id", separator=", ")
    df = join_list_in_column(df, "unii", separator=", ")

    return df


def results_to_dict(results):
    """Convert a list of results as json to a list of dictionaries with specified keys."""
    keys_o_i = [
        "indications_and_usage",
        "effective_time",
    ]

    open_fda_keys_o_i = [
        "brand_name",
        "generic_name",
        "application_number",
        "product_ndc",
        "rxcui",
        "spl_id",
        "spl_set_id",
        "unii",
    ]

    result_list = []
    for i in range(len(results)):
        res = results[i]
        res_dict = {key: res.get(key, None) for key in keys_o_i}
        res_openfda = res.get("openfda")
        for key in open_fda_keys_o_i:
            res_dict[key] = res_openfda.get(key, None)
        result_list.append(res_dict)
    return result_list


def fda_label_example(active_ingredient):
    # Example usage:
    base_url = "https://api.fda.gov/drug/label.json"
    query = f'search=openfda.substance_name:"{active_ingredient}"'
    url = f"{base_url}?{query}&limit=1"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
    else:
        data = response.json()
        results = data.get("results", [])
        if results:
            return dict(results[0])


def get_fda_submissions_by_ingredient(active_ingredient, limit=100):
    """
    Retrieve all FDA submissions (including supplements) for a given active ingredient.
    Uses the openFDA drugsfda endpoint.
    Returns a list of submission records (each linked to an application).
    """

    base_url = "https://api.fda.gov/drug/drugsfda.json"
    query = f"search=openfda.substance_name:{active_ingredient.upper()}"
    skip = 0
    all_submissions = []

    while True:
        url = f"{base_url}?{query}&limit={limit}&skip={skip}"
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f"Error {resp.status_code}: {resp.text}")

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        # Extract all submissions from each application
        for app in results:
            app_num = app.get("application_number")
            sponsor = app.get("sponsor_name")
            for sub in app.get("submissions", []) or []:
                all_submissions.append(
                    {
                        "application_number": app_num,
                        "sponsor_name": sponsor,
                        "submission_type": sub.get("submission_type"),
                        "submission_number": sub.get("submission_number"),
                        "submission_status": sub.get("submission_status"),
                        "submission_status_date": sub.get("submission_status_date"),
                        "submission_class_code": sub.get("submission_class_code"),
                        "review_priority": sub.get("review_priority"),
                    }
                )

        # Pagination logic
        meta = data.get("meta", {}).get("results", {})
        total = meta.get("total", 0)
        skip += limit
        if skip >= total:
            break

    return all_submissions


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

    # submissions = submissions[submissions["submission_status"] == "AP"]

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

    df_anxiety_meds = pd.read_csv("/Users/tunaacisu/Downloads/OWID_ Anxiety drugs dataset - data.csv")
    df_anxiety_meds = df_anxiety_meds.dropna(subset=["Drug_name"])

    # get minimum year where the drug was approved
    df_anxiety_meds["min_approval_year_anxiety"] = None
    df_anxiety_meds["notes_anxiety  "] = None

    df_anxiety_meds["min_approval_year"] = None
    df_anxiety_meds["notes"] = None

    for _idx, row in df_anxiety_meds.iterrows():
        drug_name = row["Drug_name"]
        print(drug_name)
        df_fda_label = get_fda_labels_by_ingredient(drug_name)
        if df_fda_label.empty:
            print(f"No FDA label data found for {drug_name}.")
            df_anxiety_meds.loc[_idx, "min_approval_year_anxiety"] = None
            df_anxiety_meds.loc[_idx, "notes_anxiety"] = "No FDA label data found."
            df_anxiety_meds.loc[_idx, "min_approval_year"] = None
            df_anxiety_meds.loc[_idx, "notes"] = "No FDA label data found."
            print(f"################# {drug_name} #################")
            print("No FDA label data found.")
            continue

        print("##### Indications and usage samples with NO anxiety keywords #####")
        print(
            df_fda_label[df_fda_label["indications_and_usage"].apply(no_anxiety_keywords)][
                "indications_and_usage"
            ].unique()
        )

        print("##### Indications and usage samples WITH anxiety keywords #####")
        print(
            df_fda_label[~df_fda_label["indications_and_usage"].apply(no_anxiety_keywords)][
                "indications_and_usage"
            ].unique()
        )

        anxiety_df = df_fda_label[~df_fda_label["indications_and_usage"].apply(no_anxiety_keywords)]
        print(anxiety_df[["indications_and_usage", "effective_time", "brand_name", "application_number"]])
        anxiety_approvals = anxiety_df["application_number"].unique()
        print(f"Found {len(anxiety_approvals)} anxiety-related approvals for {drug_name}.")

        min_effective_time = anxiety_df["effective_time"].min()

        print(f"Finding {drug_name} minimum approval year...")

        mask = (
            submissions["active_ingredients"]
            .astype(str)  # coerce non-strings
            .str.lower()
            .str.contains(drug_name.lower(), na=False)
        )

        mask_appl = submissions["application_number"].isin(anxiety_approvals)

        anxiety_approvals_df = submissions[mask_appl]

        if anxiety_approvals_df.empty:
            min_approval_year_anx = None
            df_anxiety_meds.loc[_idx, "min_approval_year_anxiety"] = None
            df_anxiety_meds.loc[_idx, "notes_anxiety"] = "No indication for anxiety found in the FDA database."

        else:
            min_approval_idx_anx = anxiety_approvals_df["approval_year"].idxmin()

            min_approval_brand_name_anx = anxiety_approvals_df.loc[min_approval_idx_anx, "brand_name"]
            min_approval_appli_no_anx = anxiety_approvals_df.loc[min_approval_idx_anx, "application_number"]
            min_approval_year_anx = anxiety_approvals_df.loc[min_approval_idx_anx, "approval_year"]

            df_anxiety_meds.loc[_idx, "min_approval_year_anxiety"] = min_approval_year_anx
            df_anxiety_meds.loc[_idx, "notes_anxiety"] = (
                f"Brand name: {min_approval_brand_name_anx}, \nApplication number: {min_approval_appli_no_anx}"
            )

        print(f"################# {drug_name} #################")
        print(f"min approval year for anxiety indication: {min_approval_year_anx}")
        print(f"min effective time from labels: {min_effective_time}")

        # old code
        this_drug = submissions[mask]
        # print(f"Found {len(this_drug)} products for {drug_name}.")
        if this_drug.empty:
            min_approval_year = None
            df_anxiety_meds.loc[_idx, "min_approval_year"] = None
            df_anxiety_meds.loc[_idx, "notes"] = "No products found in the FDA database."

        else:
            min_approval_idx = this_drug["approval_year"].idxmin()

            min_approval_year = this_drug.loc[min_approval_idx, "approval_year"]
            min_approval_brand_name = this_drug.loc[min_approval_idx, "brand_name"]
            min_approval_marketing_status = this_drug.loc[min_approval_idx, "marketing_status"]
            min_approval_sponsor_name = this_drug.loc[min_approval_idx, "sponsor_name"]
            min_approval_appli_no = this_drug.loc[min_approval_idx, "application_number"]

            df_anxiety_meds.loc[_idx, "min_approval_year"] = min_approval_year
            df_anxiety_meds.loc[_idx, "notes"] = (
                f"Brand name: {min_approval_brand_name}, \nMarketing status: {min_approval_marketing_status}, \nSponsor name: {min_approval_sponsor_name}, \nApplication number: {min_approval_appli_no}"
            )

        print(f"min approval: {min_approval_year} - min approval for anxiety: {min_approval_year_anx}")

    df_anxiety_meds.to_csv("/Users/tunaacisu/Data/FDA_drugs/Anxiety_drugs_approval_years_both.csv", index=False)
    print("Anxiety drugs approval years saved to CSV.")
