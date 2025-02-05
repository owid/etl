"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Number of countries in each WHO region.
WHO_REGION_MEMBERS = {
    "African Region (WHO)": 47,
    "World": 194,
    "Eastern Mediterranean (WHO)": 22,
    "European Region (WHO)": 53,
    "Region of the Americas (WHO)": 35,
    "South-East Asia Region (WHO)": 11,
    "Western Pacific Region (WHO)": 27,
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("testing_coverage")

    # Read table from meadow dataset.
    tb = ds_meadow["testing_coverage"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = format_specimen(tb)
    tb = tb.drop(columns=["min", "q1", "median", "q3", "max"])
    # A table where the specimen column is the country, to make stacked bar chart.
    tb_specimen = calculate_number_infections_not_tested_for_susceptibility(tb)

    # Pivot the table to have one row per country and year.
    tb = tb.pivot(
        index=["country", "year"],
        columns="specimen",
        values=[
            "ctas_with_reported_bcis",
            "ctas_with_reported_bcis_with_ast__gt__80_bcis",
            "total_bcis",
            "total_bcis_with_ast",
        ],
        join_column_levels_with="_",
    )
    # Add the number of countries in each WHO region to calculate the share of countries that are reporting data.
    tb = add_number_of_countries_in_each_region(tb)
    # Calculate the share of countries in each WHO region that are reporting data.
    tb = calculate_share_of_countries(tb)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_specimen], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def format_specimen(tb: Table) -> Table:
    """
    Format the syndrome column.
    """
    specimen_dict = {"BLOOD": "bloodstream", "STOOL": "stool", "URINE": "urinary_tract", "UROGENITAL": "gonorrhea"}
    tb["specimen"] = tb["specimen"].astype(str)
    tb["specimen"] = tb["specimen"].replace(specimen_dict)
    assert tb["specimen"].isin(specimen_dict.values()).all()

    return tb


def add_number_of_countries_in_each_region(tb: Table) -> Table:
    """
    Adding number of countries in each WHO region in order to calculate the share that are reporting data.
    """
    tb["number_of_countries_in_region"] = tb["country"].map(WHO_REGION_MEMBERS)
    tb["number_of_countries_in_region"] = tb["number_of_countries_in_region"].astype("Int64")
    assert tb["number_of_countries_in_region"].notnull().all(), "Missing WHO region! Check spelling."

    return tb


def calculate_share_of_countries(tb: Table) -> Table:
    """
    Calculate the share of countries in each WHO region that are reporting data.
    """
    columns_with_number_of_countries = tb.columns[tb.columns.str.startswith("ctas")]
    for column in columns_with_number_of_countries:
        new_column = "share_" + column
        tb[new_column] = (tb[column] / tb["number_of_countries_in_region"]) * 100

    tb = tb.drop(columns="number_of_countries_in_region")
    return tb


def calculate_number_infections_not_tested_for_susceptibility(tb: Table) -> Table:
    """
    Calculate the number of infections not tested for susceptibility to make stacked bar chart.
    """
    tb = tb[tb["country"] == "World"]
    tb["infections_not_tested_for_susceptibility"] = tb["total_bcis"] - tb["total_bcis_with_ast"]
    tb = tb.drop(
        columns=[
            "country",
            "ctas_with_reported_bcis",
            "ctas_with_reported_bcis_with_ast__gt__80_bcis",
            "total_bcis",
        ]
    )
    tb = tb.rename(columns={"specimen": "country"})
    tb["country"] = tb["country"].replace(
        {
            "bloodstream": "Bloodstream",
            "stool": "Stool",
            "urinary_tract": "Urinary tract",
            "gonorrhea": "Gonorrhea",
        }
    )
    tb = tb.format(["country", "year"], short_name="specimen")

    return tb
