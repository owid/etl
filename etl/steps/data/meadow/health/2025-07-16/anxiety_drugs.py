"""Load a snapshot and create a meadow dataset."""

import json
import time
import zipfile
from datetime import datetime as dt

import pandas as pd
import requests
import tqdm

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SPL_BASE_URL = "https://nctr-crs.fda.gov/fdalabel/services/spl/set-ids/"
PDF_BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/downloadpdffile.cfm?setId="
DAILYMED_BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid="

VERBOSE = True

DATETIME_TODAY = dt.now().strftime("%Y-%m-%d_%H-%M")


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


ANXIETY_KEYWORDS = ["anxiety", "Anxiety", "Panic", "panic", "GAD"]


def no_anxiety_keywords(text):
    """return True if none of the anxiety keywords are in the text (case sensitive)"""
    if not isinstance(text, str):
        return True
    return all(keyword not in text for keyword in ANXIETY_KEYWORDS)


def get_fda_label(query, limit=100):
    """
    Fetch all FDA label entries for a given active ingredient from openFDA.
    Returns a pandas DataFrame containing key fields (indications, brand_name, etc.).
    """

    base_url = "https://api.fda.gov/drug/label.json"
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
        # make progress bar with tqdm
        tqdm_bar = tqdm.tqdm(total=total_hits, desc=f"Fetching records for {query}")
        tqdm_bar.update(skip)

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


def get_fda_labels_by_characteristic(search_term, limit=100, characteristic="active ingredient"):
    """
    Fetch all FDA label entries for a given active ingredient from openFDA.
    Returns a pandas DataFrame containing key fields (indications, brand_name, etc.).
    """

    if characteristic == "active ingredient":
        query_char = "openfda.substance_name"
    elif characteristic == "indication":
        query_char = "indications_and_usage"
    else:
        raise ValueError("characteristic must be either 'active ingredient' or 'indication'")

    query = f'search={query_char}:"{search_term}"'

    df = get_fda_label(query, limit)

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
    query = f"search=openfda.substance_name:{active_ingredient}"
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


def create_label_and_submissions_tables(anxiety_drugs_list, submissions):
    labels = {}
    submissions_dict = {}
    start_time = time.time()

    for i in range(len(anxiety_drugs_list)):
        drug_name = anxiety_drugs_list[i]
        df_fda_label = get_fda_labels_by_characteristic(drug_name)
        labels[drug_name] = df_fda_label
        submissions_dict[drug_name] = submissions[
            submissions["active_ingredients"].astype(str).str.lower().str.contains(drug_name.lower(), na=False)
        ]
        if VERBOSE:
            print(f"Drug {i + 1}/{len(anxiety_drugs_list)}: {drug_name}")
            print(f"Total labels found: {len(df_fda_label)} - {time.time() - start_time:.2f} seconds elapsed.")
            print("Total submissions found:", len(submissions_dict[drug_name]))

    return labels, submissions_dict


def print_urls_and_check(spl_set_id):
    """Given a SPL set ID, print the DailyMed URLs and check if the PDF is accessible."""
    pdf_url = f"{PDF_BASE_URL}{spl_set_id}"
    daily_med_url = f"{DAILYMED_BASE_URL}{spl_set_id}"
    spl_med_url = f"{SPL_BASE_URL}{spl_set_id}"

    print(f"DailyMed URL: {daily_med_url}")
    print(f"PDF URL: {pdf_url}")
    print(f"SPL Med URL: {spl_med_url}")

    d_c = check_download(pdf_url)
    print("pdf:", (d_c["likely_download"]))
    print("\n")


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
    col_min_approval_year_anx = "First approval for drug with application number with anxiety indication"
    col_min_approval_year = "First approval for drug (overall)"
    col_min_approval_year_anx_effective_time = (
        "First approval for drug with anxiety indication based on effective time from labels"
    )

    df_anxiety_meds[col_min_approval_year] = None
    df_anxiety_meds[col_min_approval_year_anx] = None
    df_anxiety_meds[col_min_approval_year_anx_effective_time] = None

    df_anxiety_meds["notes_anxiety"] = None
    df_anxiety_meds["notes"] = None
    df_anxiety_meds["daily_med_url"] = None

    drug_names = ["Amitriptyline"]  # df_anxiety_meds["Drug_name"].unique()

    dict_fda_labels, dict_submissions = create_label_and_submissions_tables(drug_names, submissions)

    for _idx, row in df_anxiety_meds.iterrows():
        drug_name = row["Drug_name"]
        df_fda_label = dict_fda_labels[drug_name]
        submissions = dict_submissions[drug_name]

        if VERBOSE:
            print(f"##### {drug_name} #####")

        # Check if FDA label data is available: If not continue to next drug
        if df_fda_label.empty:
            df_anxiety_meds.loc[_idx, col_min_approval_year] = None
            df_anxiety_meds.loc[_idx, col_min_approval_year_anx] = None
            df_anxiety_meds.loc[_idx, col_min_approval_year_anx_effective_time] = None
            df_anxiety_meds.loc[_idx, "notes_anxiety"] = "No FDA label data found."
            df_anxiety_meds.loc[_idx, "notes"] = "No FDA label data found."

            if VERBOSE:
                print(f"################# {drug_name} #################")
                print(f"No FDA label data found for {drug_name}.")
            continue

        # sort by effective time. use most recent label to check if anxiety keywords are present in indications
        df_fda_label = dict_fda_labels[drug_name].sort_values("effective_time", ascending=True)
        first_label = df_fda_label.head(1)
        last_label = df_fda_label.tail(1)

        spl_set_id_drug = last_label.iloc[0]["spl_set_id"]
        daily_med_url = f"{DAILYMED_BASE_URL}{spl_set_id_drug}"
        df_anxiety_meds.loc[_idx, "daily_med_url"] = daily_med_url

        anxiety_label_msk = ~df_fda_label["indications_and_usage"].apply(no_anxiety_keywords)
        anxiety_df = df_fda_label[anxiety_label_msk]

        if VERBOSE:
            print(f"################# {drug_name} #################")
            print(f"Total labels found: {len(df_fda_label)}")
            print("##### Indications and usage samples with NO anxiety keywords #####")
            print(anxiety_df["indications_and_usage"].unique())
            print("##### Indications and usage samples WITH anxiety keywords #####")
            print(df_fda_label[~anxiety_label_msk]["indications_and_usage"].unique())

        # if no anxiety keywords in any of the indications, print current label and save in csv. Then continue to next drug
        if anxiety_df.empty:
            spl_set_id_drug = last_label.iloc[0]["spl_set_id"]

            print("First label: ")
            print(first_label[["effective_time", "brand_name", "application_number", "spl_set_id"]])
            print_urls_and_check(first_label.iloc[0]["spl_set_id"])

            print("Last label: ")
            print(last_label[["effective_time", "brand_name", "application_number", "spl_set_id"]])
            print_urls_and_check(last_label.iloc[0]["spl_set_id"])

            df_anxiety_meds.loc[_idx, col_min_approval_year_anx] = None
            df_anxiety_meds.loc[_idx, col_min_approval_year_anx_effective_time] = None
            df_anxiety_meds.loc[_idx, "notes_anxiety"] = "No indication for anxiety found in the FDA database."

            continue

        # Otherwise try to find oldest anxiety indication
        anxiety_approvals = anxiety_df["application_number"].unique()
        if VERBOSE:
            print(f"Found {len(anxiety_approvals)} anxiety-related approvals for {drug_name}.")

        min_effective_time = anxiety_df["effective_time"].min()
        min_effective_year = str(min_effective_time)[:4]

        print(f"Finding {drug_name} minimum approval year...")

        # get all submissions with the application numbers that have anxiety indications
        mask_appl = submissions["application_number"].isin(anxiety_approvals)
        anxiety_approvals_df = submissions[mask_appl]

        min_approval_idx_anx = anxiety_approvals_df["approval_year"].idxmin()

        min_approval_brand_name_anx = anxiety_approvals_df.loc[min_approval_idx_anx, "brand_name"]
        min_approval_appli_no_anx = anxiety_approvals_df.loc[min_approval_idx_anx, "application_number"]
        min_approval_year_anx = anxiety_approvals_df.loc[min_approval_idx_anx, "approval_year"]

        df_anxiety_meds.loc[_idx, col_min_approval_year_anx] = min_approval_year_anx
        df_anxiety_meds.loc[_idx, col_min_approval_year_anx_effective_time] = min_effective_year
        df_anxiety_meds.loc[_idx, "notes_anxiety"] = (
            f"Brand name: {min_approval_brand_name_anx}, \nApplication number: {min_approval_appli_no_anx}"
        )

        # get all submissions where the drug name is in the active ingredients
        mask = submissions.apply(lambda x: drug_name.lower() in str(x["active_ingredients"]).lower(), axis=1)
        this_drug = submissions[mask]

        # print(f"Found {len(this_drug)} products for {drug_name}.")

        if this_drug.empty:
            min_approval_year = None
            df_anxiety_meds.loc[_idx, col_min_approval_year] = None
            df_anxiety_meds.loc[_idx, "notes"] = "No products found in the FDA database."

        else:
            min_approval_idx = this_drug["approval_year"].idxmin()
            min_approval_year = this_drug.loc[min_approval_idx, "approval_year"]
            min_approval_brand_name = this_drug.loc[min_approval_idx, "brand_name"]
            min_approval_marketing_status = this_drug.loc[min_approval_idx, "marketing_status"]
            min_approval_sponsor_name = this_drug.loc[min_approval_idx, "sponsor_name"]
            min_approval_appli_no = this_drug.loc[min_approval_idx, "application_number"]

            df_anxiety_meds.loc[_idx, col_min_approval_year] = min_approval_year
            df_anxiety_meds.loc[_idx, "notes"] = (
                f"Brand name: {min_approval_brand_name}, \nMarketing status: {min_approval_marketing_status}, \nSponsor name: {min_approval_sponsor_name}, \nApplication number: {min_approval_appli_no}"
            )

        if VERBOSE:
            print(
                f"min approval: {min_approval_year} - min approval for anxiety: {min_approval_year_anx} - min effective time: {min_effective_time}"
            )

    df_anxiety_meds.to_csv(
        f"/Users/tunaacisu/Data/FDA_drugs/{DATETIME_TODAY}_Anxiety_drugs_approval_including_links.csv", index=False
    )
    print("Anxiety drugs approval years saved to CSV.")


FILE_LIKE_MIME_PREFIXES = (
    "application/",
    "audio/",
    "image/",
    "video/",
)
LIKELY_WEBPAGE_MIMES = {
    "text/html",
    "application/xhtml+xml",
}


def check_download(url, timeout=10, max_probe_bytes=2048, allow_redirects=True):
    """
    Returns a dict describing whether the URL likely triggers a file download
    and why. Avoids downloading the full content.
    """

    def decide(headers, final_url):
        cd = headers.get("Content-Disposition", "") or ""
        ctype = (headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
        clen = headers.get("Content-Length")
        # Signals
        has_attachment = "attachment" in cd.lower()
        has_filename = "filename=" in cd.lower()
        is_file_mime = any(ctype.startswith(p) for p in FILE_LIKE_MIME_PREFIXES) and ctype not in LIKELY_WEBPAGE_MIMES
        looks_like_html = ctype in LIKELY_WEBPAGE_MIMES
        looks_like_ext = any(
            final_url.lower().endswith(ext)
            for ext in (
                ".pdf",
                ".zip",
                ".csv",
                ".xlsx",
                ".xls",
                ".tar",
                ".gz",
                ".bz2",
                ".7z",
                ".png",
                ".jpg",
                ".jpeg",
                ".gif",
                ".mp3",
                ".mp4",
                ".mov",
                ".avi",
            )
        )
        likely = bool(has_attachment or has_filename or is_file_mime or looks_like_ext)
        return {
            "likely_download": likely and not looks_like_html,
            "content_type": ctype or None,
            "content_length": int(clen) if (clen and clen.isdigit()) else None,
            "content_disposition": cd or None,
            "final_url": final_url,
            "signals": {
                "has_attachment": has_attachment,
                "has_filename": has_filename,
                "is_file_mime": is_file_mime,
                "looks_like_html": looks_like_html,
                "looks_like_ext": looks_like_ext,
            },
        }

    try:
        # Try HEAD first (cheap). Many CDNs support it; some apps don't.
        r = requests.head(url, timeout=timeout, allow_redirects=allow_redirects)
        if r.status_code < 400 and r.headers:
            return decide(r.headers, r.url)

        # Fall back to a streamed GET, reading only a tiny chunk.
        with requests.get(url, timeout=timeout, allow_redirects=allow_redirects, stream=True) as g:
            g.raise_for_status()
            info = decide(g.headers, g.url)
            # Optional tiny read: sometimes servers set no headers until body starts.
            # This won’t download the whole file.
            _ = next(g.iter_content(chunk_size=max_probe_bytes), b"")
            return info

    except requests.RequestException as e:
        return {"error": str(e), "likely_download": None}
