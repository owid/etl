"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
YEAR = paths.version[:4]
COLS_TO_KEEP = {
    "Country": "country",
    "2.8.2 Country has laws or regulations on prescription and sale of antimicrobials for terrestrial animal use.": "laws_antimicrobials_terrestrial_2_8_2",
    "2.8.3 Country has laws or regulations on prescription and sale of antimicrobials for aquatic animals.": "law_antimicrobials_aquatic_2_8_3",
    "2.8.5 Country has laws or regulations that prohibits the use of antibiotics for growth promotion in terrestrial animals in the absence of risk analysis.": "law_antimicrobials_terrestrial_growth_promotion_2_8_5",
    "3.2 National monitoring system for consumption and rational use of antimicrobials in human health": "monitoring_consumption_human_3_2",
    "3.3 National surveillance system for antimicrobial resistance (AMR) in humans": "amr_surveillance_human_3_3",
    "4.5 a Do you have a national plan or system in place for monitoring sales/use of antimicrobials in animals?": "monitoring_sales_use_animals_4_5_a",
    "4.5 b  Do you submit AMU data to the WOAH Database on Antimicrobial agents intended for use in animals?": "amu_data_submission_whoah_4_5_b",
    "4.6 WOAH Reporting Options for the antimicrobial use database": "woah_reporting_options_4_6",
    "4.7 National surveillance system for antimicrobial resistance (AMR) in live terrestrial animals": "surveillance_amr_terrestrial_4_7",
    "4.8 National surveillance system for antimicrobial resistance (AMR) in live aquatic animals": "surveillance_amr_aquatic_4_8",
    "5.3 National surveillance system for antimicrobial resistance (AMR) in food (terrestrial and aquatic animal and plant origin)": "surveillance_amr_food_5_3",
    "6.3 Is there a system for regular monitoring (passive surveillance) of antimicrobial compounds and their metabolites (or residues) and resistant bacteria or antimicrobial resistance genes (ARGs) in water quality?": "monitoring_amr_water_6_3",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("tracss.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    tb["year"] = YEAR

    tb = tb.rename(columns=COLS_TO_KEEP, errors="raise")
    # Lets keep just the columns we need, the questions listed above and 'year'
    cols_to_keep = list(COLS_TO_KEEP.values()) + ["year"]
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb[cols_to_keep]
    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
