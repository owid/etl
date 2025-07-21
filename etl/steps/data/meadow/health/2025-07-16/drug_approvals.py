"""Load a snapshot and create a meadow dataset."""

import json
import zipfile

from owid.catalog import Table

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

    # Total missing submissions: 2706, 9.53% of total.
    # Total missing products: 362, 1.28% of total.
    # Total missing fda information: 16087, 56.68% of total.

    snap_meta = snap.to_table_metadata()

    tb_sub = Table(submissions, metadata=snap_meta, short_name="drugs_fda_submissions")
    tb_products = Table(products, metadata=snap_meta, short_name="drugs_fda_products")
    tb_open_fda = Table(open_fda, metadata=snap_meta, short_name="drugs_open_fda")
    return tb_sub, tb_products, tb_open_fda


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

    tb_sub, tb_products, tb_open_fda = results_to_tables(data, snap)

    #
    # Process data.
    #
    # Improve tables format.

    tb_products = tb_products.format(["application_number", "product_number"], short_name="drugs_fda_products")
    tb_open_fda = tb_open_fda.format(["application_number"], short_name="drugs_open_fda")

    tables = [tb_products, tb_open_fda]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
