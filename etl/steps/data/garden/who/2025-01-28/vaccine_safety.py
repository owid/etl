"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# A dictionary to map the original variable names to the new column names.
DESCRIPTION_SHORT = {
    "Does the country have a national system to monitor adverse events following immunization?": "national_system_monitor_adverse_events",
    "How many total adverse events, including suspected or confirmed, were reported to the national level?": "total_adverse_events_reported",
    "Is incineration a recommended practice for disposal of immunization waste?": "incineration_recommended",
    "Are there other recommended practices for disposal of immunization waste?": "other_recommended_practices",
    "Is burial a recommended practice for disposal of immunization waste?": "burial_recommended",
    "Is burning in open containers a recommended practice for disposal of immunization waste?": "burning_recommended",
    'Of the total adverse events reported, how many were "serious"?': "serious_adverse_events",
    "Does the country have a national policy for waste from immunization activities?": "national_policy_waste",
    "Does the country have a vaccine adverse events review committee?": "vaccine_adverse_events_review_committee",
    "What is the source of data for the total number of adverse events reported?": "source_data_total_adverse_events",
    "What is the source of data for the total number of serious adverse events reported?": "source_data_serious_adverse_events",
    "Is encapsulation a recommended practice for disposal of immunization waste?": "encapsulation_recommended",
    "Is engineered sanitary landfill a recommended practice for disposal of immunization waste?": "engineered_sanitary_landfill_recommended",
    "Is inertization a recommended practice for disposal of immunization waste?": "inertization_recommended",
    "Is recycling a recommended practice for disposal of immunization waste?": "recycling_recommended",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccine_safety")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccine_safety")

    #
    # Process data.
    tb = tb.rename(columns={"countryname": "country"})
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = clean_data(tb)
    tb["description"] = tb["description"].replace(DESCRIPTION_SHORT)
    tb = tb.pivot(index=["country", "year"], columns="description", values="value").reset_index()

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_data(tb: Table) -> Table:
    """
    - Drop extraneous columns
    - Replace 'ND' and 'NR' with NA
    - There are two variables for the yellow fever vaccine, which do not overlap in time-coverage.
    I believe they are just two spellings of the same vaccine, so I will merge them.
    """
    tb = tb.drop(columns=["iso_3_code", "who_region", "indcode", "indcatcode", "indcat_description", "indsort"])
    tb = tb.replace({"value": {"ND": pd.NA, "NR": pd.NA}})

    return tb
