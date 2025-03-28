"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gmh_countdown")

    # Read table from meadow dataset.
    tb = ds_meadow["gmh_countdown"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Select only the WHO Mental Health Atlas 2020 data.
    tb = tb[tb["source"] == "WHO Mental Health Atlas 2020"]

    # Pivot table
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()
    tb_pivoted = tb_pivoted.dropna(subset=["year"]).set_index(["country", "year"], verify_integrity=True)
    # Add description from producer to the metadata
    for column in tb_pivoted.columns:
        description = tb["indicator_description"][tb["indicator"] == column].iloc[0]
        # Add the description to the metadata
        tb_pivoted[column].metadata.description_from_producer = description
        tb_pivoted[column].metadata.origins[0].producer = "WHO Mental Health Atlas 2020 via UNICEF"

    # Simplify categorical values
    simplify_dict = {
        "NO": "No",
        "YES": "Yes",
        "Indicators were available and used in the last two years for monitoring and evaluating implementation of most or all components of current mental health policies / plans": "Indicators used for most/all components (last 2 years)",
        "Indicators were available but not used in the last two years for monitoring and evaluating the implementation of current mental health policies / plans": "Indicators available but not used (last 2 years)",
        "Indicators not available": "Indicators not available",
        "Indicators were available and used in the last two years for monitoring and evaluating implementation of some / a few components of current mental health policies / plans": "Indicators used for some components (last 2 years)",
        "25% or less of inpatients receive timely diagnosis, treatment and follow-up for physical health conditions (e.g. cancer, diabetes or TB) in the last two years": "≤25% inpatients receive timely care (last 2 years)",
        "More than 75% of inpatients receive timely diagnosis, treatment and follow-up for physical health conditions (e.g. cancer, diabetes or TB) in the last two years": ">75% inpatients receive timely care (last 2 years)",
        "51%-75% of inpatients receive timely diagnosis, treatment and follow-up for physical health conditions (e.g. cancer, diabetes or TB) in the last two years": "51%-75% inpatients receive timely care (last 2 years)",
        "26%-50% of inpatients receive timely diagnosis, treatment and follow-up for physical health conditions (e.g. cancer, diabetes or TB) in the last two years": "26%-50% inpatients receive timely care (last 2 years)",
        "Mental health data (either in the public system, private system or both) have been compiled for general health statistics in the last two years, but not in a specific mental health report": "Mental health data compiled for general stats, no specific report (last 2 years)",
        "No mental health data have been compiled in a report for policy, planning or management purposes in the last two years": "No mental health data compiled in report (last 2 years)",
        "A specific report focusing mental health activities in both the public and private sector has been published by the Health Department or any other responsible government unit in the last two years": "Specific report published for public and private sectors (last 2 years)",
        "A specific report focusing on mental health activities in the public sector only has been published by the Health Department or any other responsible government unit in the last two years": "Specific report published for public sector only (last 2 years)",
    }

    for column in tb_pivoted.columns:
        unique_values = tb_pivoted[column].dropna().unique()
        if any(value in simplify_dict for value in unique_values):
            tb_pivoted[column] = tb_pivoted[column].map(simplify_dict)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_pivoted], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
