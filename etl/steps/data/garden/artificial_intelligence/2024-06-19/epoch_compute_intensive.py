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
    ds_meadow = paths.load_dataset("epoch_compute_intensive")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch_compute_intensive"]
    tb = tb.reset_index()

    #
    # Process data.
    #

    # Convert relevant columns to string type
    columns = ["system", "domain", "organization_categorization"]
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
        "Government",
        "Industry and research collective collaboration",
        "Academia, government, industry and research collective collaboration",
        "Academia and industry collaboration",
        "Industry",
        "Academia",
    }
    assert unique_values == expected_values, "Unexpected affiliations in organization_categorization column"

    # Convert FLOP to petaFLOP and remove the column with FLOPs (along with training time in hours)
    tb["training_computation_petaflop"] = tb["training_compute__flop"] / 1e15

    # Convert publication date to a datetime objects
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate 'days_since_1949'
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days.astype("Int64")
    tb = tb.dropna(subset=["days_since_1949"])

    tb = tb.reset_index(drop=True)

    assert not tb[["system", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Drop columns that are not needed
    tb = tb.drop(
        ["training_compute__flop", "organization", "authors", "country__from_organization", "notability_criteria"],
        axis=1,
    )
    tb = tb.format(["days_since_1949", "system"])

    # Add metadata to the publication date column
    tb["publication_date"].metadata.origins = tb["domain"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch_compute_intensive.end")
