"""Load a meadow dataset and create a garden dataset."""
import numpy as np
import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"]
    tb = tb.reset_index()

    #
    # Process data.
    # Filter notable systems by selecting rows where 'notability_criteria' is not nan
    tb = tb[tb["notability_criteria"].notna()].reset_index(drop=True)
    tb = tb.drop("notability_criteria", axis=1)

    # Convert relevant columns to string type
    for column in ["system", "domain", "organization_categorization"]:
        tb[column] = tb[column].astype(str)

    # Function to categorize organization entries
    def categorize(entry):
        # Define mapping of keywords to categories
        categories = {
            "Academia": "Academia",
            "Industry": "Industry",
            "Research collective": "Research collective",
            "Government": "Government",
        }

        # Define special cases
        special_cases = {
            ("Academia", "Industry"): "Academia and industry collaboration",
            ("Academia", "Research collective"): "Academia and research collective collaboration",
            ("Academia", "Government"): "Academia and government collaboration",
            ("Industry", "Government"): "Industry and government collaboration",
            ("Industry", "Research collective"): "Industry and research collective collaboration",
        }

        entries = set(entry.split(","))
        matched_categories = {category for keyword, category in categories.items() if keyword in entries}

        # Check for special cases
        for keywords, category in special_cases.items():
            if set(keywords).issubset(matched_categories):
                return category

        # Check for standard cases
        if len(matched_categories) == 1:
            return next(iter(matched_categories))

        log.info(f" {entry} entry in organization column was classified as Unknown")
        return "Not specified"

    # Clean up organizations
    tb["organization_categorization"] = tb["organization_categorization"].apply(categorize)

    # Get the unique values in the organization_categorization column and compare them to expected affiliations
    unique_values = tb["organization_categorization"].unique()
    expected_values = [
        "Academia and government collaboration",
        "Academia and industry collaboration",
        "Industry",
        "Industry and government collaboration",
        "Academia",
        "Industry and research collective collaboration",
        "Not specified",
        "Academia and research collective collaboration",
        "Research collective",
        "Government",
    ]
    unique_values_set = set(unique_values)
    expected_values_set = set(expected_values)
    assert unique_values_set == expected_values_set, "Unexpected affiliations in organization_categorization column"

    # Replace nans with Unspecified in each column to avoid issues when calculating sume of notable systems
    columns = ["organization_categorization", "domain", "organization"]
    for column in columns:
        if tb[column].astype(str).str.contains("nan").any():
            tb[column] = tb[column].replace("nan", "Not specified")

    # Find domains with total number of notable systems below 10
    domain_counts = tb["domain"].value_counts()
    domains_below_10 = domain_counts[domain_counts < 10].index.tolist()

    log.info(f"Domains with less than 10 notable systems that were reclassified to Other: {domains_below_10}")
    # Rename the domains with less than 10 notable systems to 'Other'
    tb["domain_owid"] = tb["domain"].apply(lambda x: "Other" if x in domains_below_10 else x)

    # Convert FLOP to petaFLOP and remove the column with FLOPs (along with training time in hours)
    tb["training_computation_petaflop"] = tb["training_compute__flop"] / 1e15

    # Convert publication date to a datetime objects
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate 'days_since_1949'
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days
    tb = tb.dropna(subset=["days_since_1949"])

    tb = tb.reset_index(drop=True)
    tb["days_since_1949"] = tb["days_since_1949"].astype(int)

    assert not tb[["system", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Drop columns that are not needed
    tb = tb.drop(
        ["training_compute__flop", "training_time__hours", "organization", "authors", "country__from_organization"],
        axis=1,
    )
    tb = tb.set_index(["days_since_1949", "system"], verify_integrity=True).sort_index()

    # Add metadata to the publication date column
    tb["publication_date"].metadata.origins = tb["domain"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch.end")
