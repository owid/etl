"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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
        # Missing values (NaN under the pandas str dtype)
        if pd.isna(entry):
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
    tb[columns] = tb[columns].astype(str).fillna("Not specified").replace("nan", "Not specified")

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
    # Keep training compute in FLOP, as Epoch AI publishes it, and also in petaFLOP for the regression datasets.
    tb["training_compute_flop"] = tb["training_compute__flop"].copy()
    tb["training_computation_petaflop"] = tb["training_compute__flop"] / 1e15

    # Classify each model's training compute as reported by the developer or estimated by Epoch AI.
    tb = add_training_compute_reporting(tb)

    # Group each model's developers, their countries, and the availability of its weights into a few categories,
    # to color charts by.
    tb = add_developer_group(tb)
    tb = add_country_group(tb)
    tb = add_weights_availability(tb)

    # Convert training dataset size to numeric
    tb["training_dataset_size__total"] = pd.to_numeric(tb["training_dataset_size__total"], errors="coerce")

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
        [
            "training_compute__flop",
            "training_compute_estimation_method",
            "organization",
            "authors",
            "country__from_organization",
            "model_accessibility",
        ],
        axis=1,
    )
    tb = tb.format(["days_since_1949", "model"])
    # Add metadata to the publication date column
    for col in ["publication_date", "training_dataset_size__total"]:
        tb[col].metadata.origins = tb["domain"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch.end")


def add_training_compute_reporting(tb):
    """Add whether a model's training compute was reported by its developer or estimated by Epoch AI.

    Epoch AI lists one or more methods per model, e.g. "Reported,Operation counting". A model counts as
    reported when "Reported" is among them, and as estimated otherwise. Models without a compute figure get no value.
    """
    methods = tb["training_compute_estimation_method"].astype("string").fillna("").str.split(",")
    is_reported = methods.apply(lambda model_methods: "Reported" in [method.strip() for method in model_methods])
    has_compute = tb["training_compute__flop"].notna()

    tb["training_compute_reporting"] = tb["training_compute_estimation_method"].astype("object")
    tb.loc[:, "training_compute_reporting"] = None
    tb.loc[has_compute, "training_compute_reporting"] = "Estimated by Epoch AI"
    tb.loc[has_compute & is_reported, "training_compute_reporting"] = "Reported by the developer"

    # Every model with a compute figure falls in one of the two groups, and only those models do.
    assert (tb["training_compute_reporting"].notna() == has_compute).all(), "Unexpected training compute reporting."
    assert set(tb["training_compute_reporting"].dropna()) == {"Estimated by Epoch AI", "Reported by the developer"}

    return tb


# Companies shown separately when coloring by developer, with the names Epoch AI gives their organizations.
# Epoch AI lists Google's AI labs under several names; MetaMind is a Salesforce company, not Meta's.
DEVELOPER_GROUPS = {
    "OpenAI": ["OpenAI"],
    "Google": ["Google", "Google AI", "Google Brain", "Google Cloud", "Google DeepMind", "Google Research", "DeepMind"],
    "Anthropic": ["Anthropic"],
    "xAI": ["xAI"],
    "Meta": ["Meta AI", "Facebook", "Facebook AI", "Facebook AI Research"],
}

# Countries shown separately when coloring by country, with the names Epoch AI gives them.
COUNTRY_GROUPS = {
    "United States of America": "United States",
    "China": "China",
    "United Kingdom": "United Kingdom",
    "France": "France",
}

# Epoch AI's model accessibility categories, grouped by whether and how the model can be used.
WEIGHTS_AVAILABILITY = {
    "Open weights (unrestricted)": "Open weights",
    "Open weights (non-commercial)": "Open weights",
    "Open weights (restricted use)": "Open weights",
    "API access": "API or hosted access only",
    "Hosted access (no API)": "API or hosted access only",
    "Limited access": "API or hosted access only",
    "Unreleased": "Not released",
}


def _split_list(value) -> list[str]:
    """Split one of Epoch AI's comma-separated lists, dropping blanks and duplicates."""
    if pd.isna(value) or value in ("", "nan", "Not specified"):
        return []
    return list(dict.fromkeys(item.strip() for item in str(value).split(",") if item.strip()))


def add_developer_group(tb):
    """Add the company that developed each model: one of DEVELOPER_GROUPS, or "Other".

    A model counts toward a company when one of its developers is that company, even alongside universities or other
    organizations. A model developed by several of these companies, or by none of them, counts as "Other".
    """
    group_of = {
        organization: group for group, organizations in DEVELOPER_GROUPS.items() for organization in organizations
    }
    values = []
    for organizations in tb["organization"]:
        groups = {group_of[organization] for organization in _split_list(organizations) if organization in group_of}
        values.append(groups.pop() if len(groups) == 1 else "Other")

    tb["developer_group"] = tb["organization"].astype("object")
    tb.loc[:, "developer_group"] = values
    assert set(tb["developer_group"]) <= set(DEVELOPER_GROUPS) | {"Other"}, "Unexpected developer group."
    for group in DEVELOPER_GROUPS:
        assert (tb["developer_group"] == group).any(), f"No notable model developed by {group}: has its name changed?"

    return tb


def add_country_group(tb):
    """Add the country of the organizations that developed each model: one of COUNTRY_GROUPS, "Other",
    "Multiple countries", or no value when Epoch AI lists no country.

    Epoch AI marks organizations associated with several countries as "Multinational", so those count as
    "Multiple countries" too.
    """
    values = []
    for countries in tb["country__from_organization"]:
        countries = _split_list(countries)
        if not countries:
            values.append(None)
        elif len(countries) > 1 or countries[0] == "Multinational":
            values.append("Multiple countries")
        else:
            values.append(COUNTRY_GROUPS.get(countries[0], "Other"))

    tb["country_group"] = tb["country__from_organization"].astype("object")
    tb.loc[:, "country_group"] = values
    expected = set(COUNTRY_GROUPS.values()) | {"Other", "Multiple countries"}
    assert set(tb["country_group"].dropna()) == expected, "Unexpected or missing country group."

    return tb


def add_weights_availability(tb):
    """Add whether each model's weights are open, whether it can only be used through an API or another hosted
    service, or whether it was not released. Models whose accessibility Epoch AI hasn't recorded get no value.
    """
    accessibility = tb["model_accessibility"].astype("object")
    unexpected = set(accessibility.dropna()) - set(WEIGHTS_AVAILABILITY)
    assert not unexpected, f"Unexpected model accessibility values: {unexpected}"

    tb["weights_availability"] = accessibility
    tb.loc[:, "weights_availability"] = accessibility.map(WEIGHTS_AVAILABILITY)

    return tb
