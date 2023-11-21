"""Load a meadow dataset and create a garden dataset."""

import education_lee_lee
import owid.catalog.processing as pr
import shared
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "World",
]


def add_data_for_regions(tb: Table, ds_regions: Dataset) -> Table:
    tb_with_regions = tb.copy()
    aggregations = {column: "mean" for column in tb_with_regions.columns if column not in ["country", "year"]}

    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
        )
        tb_with_regions = shared.add_region_aggregates_education(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
            aggregations=aggregations,
        )
    for column in tb_with_regions.columns:
        if column not in ["year", "country"]:
            # Add origins metadata
            tb_with_regions[column].metadata.origins.append(tb[column].metadata.origins[0])

    return tb_with_regions


def run(dest_dir: str) -> None:
    # Load dependencies.
    # These datasets contain information required for the calculations.

    # Load dataset containing Barro-Lee education projections.
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_barro_lee_projections")

    # Read table from meadow dataset.
    tb = ds_meadow["education_barro_lee_projections"].reset_index()

    # Load dataset containing regions data.
    ds_regions = paths.load_dataset("regions")

    # Process data.

    # Harmonize the country names in the table.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Rename the age groups for clarity.
    tb["age_group"] = tb["age_group"].replace(
        {"15-64": "Youth and Adults (15-64 years)", "15-24": "Youth (15-24 years)", "25-64": "Adults (25-64 years)"}
    )

    # Prepare attainment data using the Lee-Lee method.
    tb_projections = education_lee_lee.prepare_attainment_data(tb)

    # Drop columns containing thousands values as they are not needed.
    columns_to_drop = [column for column in tb_projections.columns if "__thousands" in column]
    tb_projections = tb_projections.drop(columns=columns_to_drop)

    # Add regional and income group data to the projections.
    tb_projections = add_data_for_regions(tb=tb_projections, ds_regions=ds_regions)
    tb_projections = tb_projections.underscore()

    # Create a copy of the projections table with a suffix in the column names.
    tb_projections_copy = tb_projections.copy(deep=True)
    tb_projections_copy = tb_projections_copy.set_index(["country", "year"], verify_integrity=True)

    suffix = "_projections"
    tb_projections_copy.columns = tb_projections_copy.columns + suffix
    tb_projections_copy = tb_projections_copy.reset_index()

    # Load historical education data and drop columns related to enrollment rates.
    ds_past = paths.load_dataset("education_lee_lee")
    tb_past = ds_past["education_lee_lee"].reset_index()
    cols_to_drop = [col for col in tb_past.columns if "enrollment_rates" in col]
    tb_past = tb_past.drop(columns=cols_to_drop)

    # Concatenate the projections with historical data below the year 2015.
    tb_below_2015 = tb_past[tb_past["year"] < 2015]
    stiched = pr.concat([tb_projections, tb_below_2015])

    # Merge the original projections and the concatenated data.
    tb_stiched = pr.merge(
        tb_projections_copy,
        stiched,
        on=["country", "year"],
        how="outer",
    )
    # Create share with some formal education indicators
    tb_stiched["some_formal_education_female"] = (
        100 - tb_stiched["f_youth_and_adults__15_64_years__percentage_of_no_education"]
    )
    tb_stiched["some_formal_education_male"] = (
        100 - tb_stiched["m_youth_and_adults__15_64_years__percentage_of_no_education"]
    )
    tb_stiched["some_formal_education_both_sexes"] = (
        100 - tb_stiched["mf_youth_and_adults__15_64_years__percentage_of_no_education"]
    )

    # Create female to male ratios for key variables
    tb_stiched["female_over_male_average_years_of_schooling"] = (
        tb_stiched["f_youth_and_adults__15_64_years__average_years_of_education"]
        / tb_stiched["m_youth_and_adults__15_64_years__average_years_of_education"]
    )

    tb_stiched["female_over_male_share_with_no_education"] = (
        tb_stiched["f_youth_and_adults__15_64_years__percentage_of_no_education"]
        / tb_stiched["m_youth_and_adults__15_64_years__percentage_of_no_education"]
    )

    tb_stiched["female_over_male_share_some_formal_education"] = (
        tb_stiched["some_formal_education_female"] / tb_stiched["some_formal_education_male"]
    )

    # Set metadata and format the dataframe for saving.
    tb_stiched = tb_stiched.underscore().set_index(["country", "year"], verify_integrity=True)

    # Save columns to use on grapher
    columns_to_use_on_grapher = [
        "mf_youth_and_adults__15_64_years__percentage_of_no_education",
        "f_youth_and_adults__15_64_years__percentage_of_no_education",
        "f_adults__25_64_years__percentage_of_no_education",
        "mf_adults__25_64_years__percentage_of_tertiary_education",
        "mf_youth_and_adults__15_64_years__average_years_of_education",
        "f_youth_and_adults__15_64_years__average_years_of_education",
        "female_over_male_average_years_of_schooling",
        "female_over_male_share_with_no_education",
        "female_over_male_share_some_formal_education",
        "some_formal_education_female",
        "some_formal_education_male",
        "some_formal_education_both_sexes",
    ]

    tb_stiched = tb_stiched[columns_to_use_on_grapher]

    # Set metadata and format the dataframe for saving.
    tb_stiched.metadata.short_name = paths.short_name

    #
    # Save outputs
    #
    # Create a new garden dataset with the same metadata as the meadow dataset
    ds_garden = create_dataset(dest_dir, tables=[tb_stiched], check_variables_metadata=True)

    # Save the newly created dataset.
    ds_garden.save()
