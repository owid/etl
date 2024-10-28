"""Load a meadow dataset and create a garden dataset."""

from typing import Dict

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RESPONSES = {
    "monitoring_consumption_human_3_2": {
        "A - No national plan or system for monitoring use of antimicrobials. ": "No monitoring system",
        "B - System designed for surveillance of antimicrobial use, that includes monitoring national level sales† or use  of antibiotics in health services. ": "National monitoring of antimicrobial sales and antibiotic use in health services",
        "C - Total sales of antimicrobials are monitored at national level and/or some monitoring of antibiotic use at sub-national level. ": "National monitoring of antimicrobial sales and some monitoring of antibiotic use at sub-national level",
        "D - Prescribing practices and appropriate antibiotic use are monitored in a national sample of healthcare settings. ": "Prescribing practices and appropriate antibiotic use monitored in a sample of healthcare settings",
        "E - On a regular basis (every year/two years) data is collected and reported on: a) and b) - refer to questionnaire for complete text": "Regular data collection and reporting",
    },
    "amr_surveillance_human_3_3": {
        "A - No capacity for generating data (antibiotic susceptibility testing and accompanying clinical and epidemiological data) and reporting on antibiotic resistance.  ": "No capacity for generating data on antibiotic resistance",
        "B - AMR data is collated locally for common† bacterial infections in hospitalized and community patients‡, but data collection may not use a standardized approach and lacks national coordination and/or quality management. ": "Local collation of AMR data for common bacterial infections",
        "C - AMR data are collated nationally for common bacterial infections in hospitalized and community patients, but national coordination and standardization are lacking. ": "National collation of AMR data for common bacterial infections",
        "D - There is a standardized national AMR surveillance system collecting data on common bacterial infections in hospitalized and community patients, with established network of surveillance sites, designated national reference laboratory for AMR, and a national coordinating centre producing reports on AMR. ": "Standardized national AMR surveillance system",
        "E - The national AMR surveillance system links AMR surveillance with antimicrobial consumption and/or use data for human health§. ": "National AMR surveillance system linked with antimicrobial consumption data",
    },
    "woah_reporting_options_4_6": {
        "A - WOAH Reporting Option: Baseline information: On a regular basis, only baseline information is reported to the WOAH ": "Baseline information only",
        "B - WOAH Reporting option 1:  On a regular basis, data is collected and reported to the WOAH on the overall amount sold for use/used in animals by antimicrobial class, with the possibility to separate by type of use.   ": "Information on amount sold for use/used in animals by antimicrobial class and type of use",
        "C - WOAH Reporting option 2:  On a regular basis, data is collected and reported to the WOAH on the overall amount sold for use/used in animals by antimicrobial class, with the possibility to separate by type of use and animal group.   ": "Information on amount sold for use/used in animals by antimicrobial class, type of use and animal group",
        "D - WOAH Reporting option 3:  On a regular basis, data is collected and reported to the WOAH on the overall amount sold for use/used in animals by antimicrobial class, with the possibility to separate by type of use, animal group and route of administration.   ": "Information on amount sold for use/used in animals by antimicrobial class, type of use, animal group and route of administration",
        "E - Data on antimicrobials used under veterinary supervision in animals are available for individual animal species. ": "Antimicrobials used under veterinary supervision in animals are available for individual animal species",
    },
    "surveillance_amr_terrestrial_4_7": {
        "A - There are no local or national strategies/plans for generating AMR surveillance data from animals for an AMR surveillance system.  ": "No national plan for AMR surveillance system",
        "B - National plan for AMR surveillance in place but laboratory and epidemiology capacities for generating, analysing and reporting data are lacking.  ": "National plan for AMR surveillance but lacking capacities",
        "C - Some AMR data is collected at local levels but a nationally standardized approach is not used. National coordination and/or quality management is lacking.  ": "AMR data collected at local levels but no standardized approach",
        "D - Priority pathogenic/ commensal bacterial species have been identified for surveillance. Data systematically collected and reported on levels of resistance in at least one of those bacterial species, involving a laboratory that follows quality management processes e.g. proficiency testing. ": "Priority bacterial species identified for surveillance",
        "E - National system of AMR surveillance established for priority animal pathogens, zoonotic and commensal bacterial isolates which follows quality assurance processes in line with intergovernmental standards. Laboratories that report for AMR surveillance follow quality assurance processes. ": "National system of AMR surveillance established for priority animal pathogens, high quality assurance processes",
    },
    "surveillance_amr_aquatic_4_8": {
        "A - There are no local or national strategies/plans for generating AMR surveillance data from aquatic  animals for an AMR surveillance system.  ": "No national plan for AMR surveillance system",
        "B - National plan for AMR surveillance in place but laboratory and epidemiology capacities for generating, analysing and reporting data are lacking.  ": "National plan for AMR surveillance but lacking capacities",
        "C - Some AMR data is collected at local levels but a nationally standardized approach is not used. National coordination and/or quality management is lacking.  ": "AMR data collected at local levels but no standardized approach",
        "D - Priority pathogenic/ commensal bacterial species have been identified for surveillance. Data systematically collected and reported on levels of resistance in at least one of those bacterial species, involving a laboratory that follows quality management processes e.g. proficiency testing. ": "Priority bacterial species identified for surveillance",
        "E - National system of AMR surveillance established for priority animal pathogens, zoonotic and commensal bacterial isolates which follows quality assurance processes in line with intergovernmental standards. Laboratories that report for AMR surveillance follow quality assurance processes. ": "National system of AMR surveillance established for priority animal pathogens, high quality assurance processes",
    },
    "surveillance_amr_food_5_3": {
        "A - No national plan for an AMR surveillance system.   ": "No national plan for AMR surveillance system",
        "B - National plan for AMR surveillance in place but capacity (including laboratory and reporting) is lacking. ": "National plan for AMR surveillance but lacking capacities",
        "C - Some AMR data is collected - but a standardized approach is not used. National coordination and/or quality management is lacking.  ": "AMR data collected but no standardized approach",
        "D - Priority food borne pathogenic/ indicator bacterial species have been identified for surveillance. Data systematically collected and reported on levels of resistance in at least one of those bacterial species, involving a laboratory that follows quality management processes e.g. proficiency testing. ": "Priority foodborne pathogens identified for surveillance",
        "E - National system of AMR surveillance established for priority foodborne pathogens and/or relevant indicator bacteria which follows quality assurance processes in line with intergovernmental standards. Laboratories that report for AMR surveillance follow quality assurance processes. ": "National system of AMR surveillance established for priority foodborne pathogens, high quality assurance processes",
    },
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tracss")

    # Read table from meadow dataset.
    tb = ds_meadow["tracss"].reset_index()
    # Save origins for re-adding later
    origins = tb["laws_antimicrobials_terrestrial_2_8_2"].metadata.origins
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = shorten_survey_responses(tb, RESPONSES)
    tb = remove_spaces(tb)
    tb = tb.format(["country", "year"])
    # Adding origins back in to the columns
    for col in tb.columns:
        tb[col].metadata.origins = origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def shorten_survey_responses(tb: Table, responses_dict: Dict) -> Table:
    """
    Shortening the survey responses to make them more readable.
    """
    for response in responses_dict:
        check_keys_exist(tb, responses_dict[response], response)
        tb[response] = tb[response].cat.rename_categories(responses_dict[response])

    return tb


def check_keys_exist(tb: Table, dict: Dict, col: str) -> None:
    current_categories = set(tb[col].cat.categories)
    missing_keys = set(dict.keys()) - current_categories

    if missing_keys:
        raise ValueError(f"Some categories in the dictionary do not exist in the Series: {missing_keys}")


def remove_spaces(tb: Table) -> Table:
    # Some columns have blank spaces, remove the additional spaces
    # Skip country and year

    for col in tb.columns[2:]:
        tb[col] = tb[col].astype(str)
        tb[col] = tb[col].replace(r"^\s*$", pd.NA, regex=True)
    return tb
