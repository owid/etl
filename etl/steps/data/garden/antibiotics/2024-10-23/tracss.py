"""Load a meadow dataset and create a garden dataset."""

from typing import Dict

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tracss")

    # Read table from meadow dataset.
    tb = ds_meadow["tracss"].reset_index()
    origins = tb["laws_antimicrobials_terrestrial_2_8_2"].metadata.origins
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = shorten_survey_responses(tb)
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


def shorten_survey_responses(tb: Table) -> Table:
    monitoring_consumption_human_3_2_responses = {
        "A - No national plan or system for monitoring use of antimicrobials. ": "No monitoring system",
        "B - System designed for surveillance of antimicrobial use, that includes monitoring national level sales† or use  of antibiotics in health services. ": "National monitoring of antimicrobial sales and antibiotic use in health services",
        "C - Total sales of antimicrobials are monitored at national level and/or some monitoring of antibiotic use at sub-national level. ": "National monitoring of antimicrobial sales and some monitoring of antibiotic use at sub-national level",
        "D - Prescribing practices and appropriate antibiotic use are monitored in a national sample of healthcare settings. ": "Prescribing practices and appropriate antibiotic use monitored in a sample of healthcare settings",
        "E - On a regular basis (every year/two years) data is collected and reported on: a) and b) - refer to questionnaire for complete text": "Regular data collection and reporting",
    }
    check_keys_exist(tb, monitoring_consumption_human_3_2_responses, "monitoring_consumption_human_3_2")
    tb["monitoring_consumption_human_3_2"] = tb["monitoring_consumption_human_3_2"].cat.rename_categories(
        monitoring_consumption_human_3_2_responses
    )

    amr_surveillance_human_3_3_responses = {
        "A - No capacity for generating data (antibiotic susceptibility testing and accompanying clinical and epidemiological data) and reporting on antibiotic resistance.  ": "No capacity for generating data on antibiotic resistance",
        "B - AMR data is collated locally for common† bacterial infections in hospitalized and community patients‡, but data collection may not use a standardized approach and lacks national coordination and/or quality management. ": "Local collation of AMR data for common bacterial infections",
        "C - AMR data are collated nationally for common bacterial infections in hospitalized and community patients, but national coordination and standardization are lacking. ": "National collation of AMR data for common bacterial infections",
        "D - There is a standardized national AMR surveillance system collecting data on common bacterial infections in hospitalized and community patients, with established network of surveillance sites, designated national reference laboratory for AMR, and a national coordinating centre producing reports on AMR. ": "Standardized national AMR surveillance system",
        "E - The national AMR surveillance system links AMR surveillance with antimicrobial consumption and/or use data for human health§. ": "National AMR surveillance system linked with antimicrobial consumption data",
    }
    check_keys_exist(tb, amr_surveillance_human_3_3_responses, "amr_surveillance_human_3_3")
    tb["amr_surveillance_human_3_3"] = tb["amr_surveillance_human_3_3"].cat.rename_categories(
        amr_surveillance_human_3_3_responses
    )

    surveillance_amr_terrestrial_4_7_responses = {
        "A - There are no local or national strategies/plans for generating AMR surveillance data from animals for an AMR surveillance system.  ": "No national plan for AMR surveillance system",
        "B - National plan for AMR surveillance in place but laboratory and epidemiology capacities for generating, analysing and reporting data are lacking.  ": "National plan for AMR surveillance but lacking capacities",
        "C - Some AMR data is collected at local levels but a nationally standardized approach is not used. National coordination and/or quality management is lacking.  ": "AMR data collected at local levels but no standardized approach",
        "D - Priority pathogenic/ commensal bacterial species have been identified for surveillance. Data systematically collected and reported on levels of resistance in at least one of those bacterial species, involving a laboratory that follows quality management processes e.g. proficiency testing. ": "Priority bacterial species identified for surveillance",
        "E - National system of AMR surveillance established for priority animal pathogens, zoonotic and commensal bacterial isolates which follows quality assurance processes in line with intergovernmental standards. Laboratories that report for AMR surveillance follow quality assurance processes. ": "National system of AMR surveillance established for priority animal pathogens, high quality assurance processes",
    }
    check_keys_exist(tb, surveillance_amr_terrestrial_4_7_responses, "surveillance_amr_terrestrial_4_7")
    tb["surveillance_amr_terrestrial_4_7"] = tb["surveillance_amr_terrestrial_4_7"].cat.rename_categories(
        surveillance_amr_terrestrial_4_7_responses
    )

    surveillance_amr_aquatic_4_8_responses = {
        "A - There are no local or national strategies/plans for generating AMR surveillance data from aquatic  animals for an AMR surveillance system.  ": "No national plan for AMR surveillance system",
        "B - National plan for AMR surveillance in place but laboratory and epidemiology capacities for generating, analysing and reporting data are lacking.  ": "National plan for AMR surveillance but lacking capacities",
        "C - Some AMR data is collected at local levels but a nationally standardized approach is not used. National coordination and/or quality management is lacking.  ": "AMR data collected at local levels but no standardized approach",
        "D - Priority pathogenic/ commensal bacterial species have been identified for surveillance. Data systematically collected and reported on levels of resistance in at least one of those bacterial species, involving a laboratory that follows quality management processes e.g. proficiency testing. ": "Priority bacterial species identified for surveillance",
        "E - National system of AMR surveillance established for priority animal pathogens, zoonotic and commensal bacterial isolates which follows quality assurance processes in line with intergovernmental standards. Laboratories that report for AMR surveillance follow quality assurance processes. ": "National system of AMR surveillance established for priority animal pathogens, high quality assurance processes",
    }
    check_keys_exist(tb, surveillance_amr_aquatic_4_8_responses, "surveillance_amr_aquatic_4_8")
    tb["surveillance_amr_aquatic_4_8"] = tb["surveillance_amr_aquatic_4_8"].cat.rename_categories(
        surveillance_amr_aquatic_4_8_responses
    )

    surveillance_amr_food_5_3_responses = {
        "A - No national plan for an AMR surveillance system.   ": "No national plan for AMR surveillance system",
        "B - National plan for AMR surveillance in place but capacity (including laboratory and reporting) is lacking. ": "National plan for AMR surveillance but lacking capacities",
        "C - Some AMR data is collected - but a standardized approach is not used. National coordination and/or quality management is lacking.  ": "AMR data collected but no standardized approach",
        "D - Priority food borne pathogenic/ indicator bacterial species have been identified for surveillance. Data systematically collected and reported on levels of resistance in at least one of those bacterial species, involving a laboratory that follows quality management processes e.g. proficiency testing. ": "Priority foodborne pathogens identified for surveillance",
        "E - National system of AMR surveillance established for priority foodborne pathogens and/or relevant indicator bacteria which follows quality assurance processes in line with intergovernmental standards. Laboratories that report for AMR surveillance follow quality assurance processes. ": "National system of AMR surveillance established for priority foodborne pathogens, high quality assurance processes",
    }
    check_keys_exist(tb, surveillance_amr_food_5_3_responses, "surveillance_amr_food_5_3")
    tb["surveillance_amr_food_5_3"] = tb["surveillance_amr_food_5_3"].cat.rename_categories(
        surveillance_amr_food_5_3_responses
    )

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
        tb[col] = tb[col].cat.rename_categories({" ": ""})
    return tb
