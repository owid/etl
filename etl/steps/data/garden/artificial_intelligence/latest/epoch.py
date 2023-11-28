"""Load a meadow dataset and create a garden dataset."""
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
    for column in ["system", "domain", "organization_categorization", "approach"]:
        tb[column] = tb[column].astype(str)

    # Clean up researcher affiliation in column 'organization_categorization'
    organization_mapping = {
        "Industry - Academia Collaboration": "Academia and industry collaboration",
        "Industry - Academia collaboration": "Academia and industry collaboration",
        "Industry - Academia Collaboration (Academia leaning)": "Academia and industry collaboration",
        "Industry - Academia Collaboration (Academia Leaning)": "Academia and industry collaboration",
        "Industry - Academia Collaboration (Industry Leaning)": "Academia and industry collaboration",
        "Industry - Academia Collaboration (Industry leaning)": "Academia and industry collaboration",
        "Research Collective": "Other",
        "Research collective": "Other",
        "Government": "Other",
        "Non-profit": "Other",
    }

    tb["organization_categorization"] = tb["organization_categorization"].replace(organization_mapping)

    # Clean up system names
    tb["system"] = tb["system"].replace({"Univeristy": "University", "Nvidia": "NVIDIA"}, regex=True)

    # There is a typo in the domain name "VIsion"
    tb["domain"] = tb["domain"].replace({"VIsion": "Vision"})

    # Ensure 'organization_categorization' and 'domain'
    for column in ["organization_categorization", "domain", "organization", "approach"]:
        tb[column] = tb[column].replace({"nan": "Not specified"})

    # Make the domain categories more concise.
    domain_mapping = {
        "3D reconstruction": "Other",
        "Driving": "Other",
        "Other": "Other",
        "Video": "Other",
        "Text-to-Video": "Other",
        "Search": "Other",
        "Audio": "Other",
        "Robotics": "Other",
    }

    tb["domain"] = tb["domain"].replace(domain_mapping)

    # Convert FLOP to petaFLOP and remove the column with FLOPs (along with training time in hours)
    tb["training_computation_petaflop"] = tb["training_compute__flop"] / 1e15

    #  Convert publication date to a datetime objects
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate 'days_since_1949'
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days
    tb = tb.dropna(subset=["days_since_1949"])

    tb = tb.reset_index(drop=True)
    tb["days_since_1949"] = tb["days_since_1949"].astype(int)

    assert not tb[["system", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Drop columns that are not needed
    tb = tb.drop(["training_compute__flop", "training_time__hours", "organization"], axis=1)
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
