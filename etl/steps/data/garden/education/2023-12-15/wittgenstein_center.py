"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wittgenstein_center")

    # Read table from meadow dataset.
    tb = ds_meadow["wittgenstein_center_data"].reset_index()

    # Load garden historical OECD dataset.
    ds_oecd = paths.load_dataset("oecd_education")
    tb_oecd = ds_oecd["oecd_education"].reset_index()
    tb_oecd_formal_ed = tb_oecd[["country", "year", "no_formal_education", "population_with_basic_education"]]

    # Filter the for years above 2020 (New Wittgenstein Center data starts at 2020)
    tb_below_2020 = tb_oecd_formal_ed[tb_oecd_formal_ed["year"] < 2020].reset_index(drop=True)

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Filter the dataset for individuals aged 15 and older and with 'No Education'
    age_15_and_above = tb["age_group"].apply(lambda x: x not in ["0-4", "5-9", "10-14"])
    no_education = tb["educational_attainment"] == "No Education"
    filtered_df = tb[age_15_and_above & no_education]

    # Calculate the share of people 15+ with no formal education for each country and year
    # First, calculate the total population aged 15+ for each country and year
    total_population_15_plus = tb[age_15_and_above].groupby(["country", "year"])["population"].sum()

    # Then, calculate the population with no formal education for each country and year
    no_education_population = filtered_df.groupby(["country", "year"])["population"].sum()

    # Calculate the share
    share_no_education = (no_education_population / total_population_15_plus) * 100

    # Create a yearly global estimate
    # Sum up the total population aged 15+ and no education population for each year
    global_total_population_15_plus = total_population_15_plus.groupby("year").sum()
    global_no_education_population = no_education_population.groupby("year").sum()

    # Calculate the global share for each year
    global_share_no_education = (global_no_education_population / global_total_population_15_plus) * 100
    # Renaming the columns for clarity
    share_no_education = share_no_education.rename("no_formal_education")
    global_share_no_education = global_share_no_education.rename("no_formal_education")

    # Resetting the index to prepare for concatenation
    share_no_education = share_no_education.reset_index()
    global_share_no_education = global_share_no_education.reset_index()
    global_share_no_education["country"] = "World"

    tb_combined = pr.concat([global_share_no_education, share_no_education])
    tb_combined["population_with_basic_education"] = 100 - tb_combined["no_formal_education"]
    tb_combined_with_oecd = pr.merge(
        tb_combined,
        tb_below_2020,
        on=["country", "year", "no_formal_education", "population_with_basic_education"],
        how="outer",
    )
    tb_combined_with_oecd = tb_combined_with_oecd.set_index(["country", "year"], verify_integrity=True)
    tb_combined_with_oecd.metadata = tb.metadata

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined_with_oecd], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
