"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("epoch.start")

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
    #
    # Filter notable systems by selecting rows where 'notability_criteria' is not nan
    tb = tb[tb["notability_criteria"].notna()].reset_index(drop=True)
    tb = tb.drop("notability_criteria", axis=1)

    # Convert relevant columns to string type
    columns = ["model", "domain", "organization_categorization"]
    tb[columns] = tb[columns].astype(str)

    def simplify_entry(entry):
        """
        Simplifies an entry of organization categories which can include many entries of Industry, Academia etc.
        Removes duplicates, ensures all words except the first one start with a lower case letter,and joins the categories with ", " and " and " before the last one.
        """
        # Check for "nan"
        if entry == "nan":
            return "Not specified"

        # Split the entry into categories, convert to set to remove duplicates
        categories = sorted(set(entry.split(",")))

        # Make sure all words except the first one start with a lower case letter
        categories = [categories[0]] + [category.lower() for category in categories[1:]]

        # Join the categories with ", " and " and " before the last one
        if len(categories) > 1:
            simplified_entry = ", ".join(categories[:-1]) + " and " + categories[-1] + " collaboration"
        else:
            simplified_entry = categories[0]

        return simplified_entry

    tb["organization_categorization"] = tb["organization_categorization"].apply(simplify_entry)

    # Get the unique values in the organization_categorization column and compare them to expected affiliations
    unique_values = set(tb["organization_categorization"])
    expected_values = {
        "Industry",
        "Academia",
        "Government",
        "Academia and industry collaboration",
        "Academia and research collective collaboration",
        "Industry and research collective collaboration",
        "Academia, industry and research collective collaboration",
        "Government and industry collaboration",
        "Research collective",
        "Academia, government and industry collaboration",
        "Academia and government collaboration",
        "Academia, government, industry and research collective collaboration",
        "Not specified",
    }
    assert unique_values == expected_values, "Unexpected affiliations in organization_categorization column"

    # Replace affiliation of researchers with less than 20 systems with 'Other'
    affiliation_counts = tb["organization_categorization"].value_counts()

    tb["organization_categorization"] = tb["organization_categorization"].where(
        tb["organization_categorization"].map(affiliation_counts) >= 20, "Other"
    )
    # Get the organizations that were reclassified to 'Other'
    reclassified_organizations = affiliation_counts[affiliation_counts < 20].index.tolist()

    paths.log.info(
        f"Affiliations of researchers with less than 20 notable systems that were reclassified to 'Other': {', '.join(reclassified_organizations)}"
    )

    # Replace nans with Unspecified in each column to avoid issues when calculating sume of notable systems
    columns = ["organization_categorization", "domain", "organization"]
    tb[columns] = tb[columns].replace("nan", "Not specified")

    # Check for multiple entries in 'domain' separated by comma
    multiple_domains = tb["domain"].str.contains(",")
    # Replace entries in 'domain' that contain a comma with 'Multiple Domains'
    tb.loc[multiple_domains, "domain"] = "Multiple domains"

    # Replace domains with less than 20 systems with 'Other'
    domain_counts = tb["domain"].value_counts()

    tb["domain"] = tb["domain"].where(tb["domain"].map(domain_counts) >= 20, "Other")
    # Get the domains that were reclassified to 'Other'
    reclassified_domains = domain_counts[domain_counts < 20].index.tolist()

    paths.log.info(
        f"Domains with less than 20 notable systems that were reclassified to 'Other': {', '.join(reclassified_domains)}"
    )
    # Convert FLOP to petaFLOP and remove the column with FLOPs (along with training time in hours)
    tb["training_computation_petaflop"] = tb["training_compute__flop"] / 1e15

    # Convert publication date to a datetime objects
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate 'days_since_1949'
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days.astype("Int64")
    tb = tb.dropna(subset=["days_since_1949"])

    tb["year"] = tb["publication_date"].dt.year

    # Replace "Transformer" with "Transformer (year)"
    tb["model"] = tb.apply(
        lambda row: f"Transformer ({row['year']})" if row["model"] == "Transformer" else row["model"], axis=1
    )

    # Remove the year column
    tb = tb.drop(columns=["year"])

    tb = tb.reset_index(drop=True)

    assert not tb[["model", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Drop columns that are not needed
    tb = tb.drop(
        ["training_compute__flop", "organization", "authors", "country__from_organization"],
        axis=1,
    )
    tb = tb.format(["days_since_1949", "model"])

    # Add metadata to the publication date column
    tb["publication_date"].metadata.origins = tb["domain"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch.end")
