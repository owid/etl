"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions for which aggregates will be created.
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]

# Custom regions for specific places where famines occured
CUSTOM_REGION_DICT = {
    "Persia": "Asia",
    "Congo Free State": "Africa",
    "Sudan, Ethiopia": "Africa",
    "Ottoman Empire": "Asia",
    "East Africa": "Africa",
    "Somaliland": "Africa",
    "African Red Sea Region": "Africa",
    "Sahel": "Africa",
    "German East Africa": "Africa",
    "Serbia, Balkans": "Europe",
    "Greater Syria": "Asia",
    "Russia, Ukraine": "Asia",
    "USSR (Southern Russia & Ukraine)": "Asia",
    "Russia, Kazakhstan": "Asia",
    "Germany, USSR": "Asia",
    "East Asia": "Asia",
    "India, Bangladesh": "Asia",
    "Eastern Europe": "Europe",
    "USSR": "Asia",
    "Somaliland, African Red Sea Region": "Africa",
    "USSR (Kazakhstan)": "Asia",
    "USSR (Southern Russia)": "Asia",
    "German occupied USSR ": "Asia",
    "Poland (ghettos and concentration camps)": "Europe",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("famines")

    # Read regions
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow.read("famines")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Exclude famines with less than 100,000 deaths (error in the spreadhsheet and should be excluded)
    tb = tb[tb["wpf_authoritative_mortality_estimate"] >= 100000]

    # Add regions to the table.
    tb = add_regions(tb, ds_regions)

    # Ensure there are no NaNs in the 'region' column
    assert not tb["region"].isna().any(), "There are NaN values in the 'region' column"

    # Split and convert the 'date' column to lists of integers
    tb["date"] = tb["date"].astype(str)
    tb["date_list"] = tb["date"].apply(lambda x: [int(year) for year in x.split(",")])

    # Create a new column 'date_range' with the minimum and maximum years
    tb["date_range"] = tb["date_list"].apply(lambda x: f"{min(x)}" if min(x) == max(x) else f"{min(x)}-{max(x)}")
    tb["simplified_place"] = tb["simplified_place"].astype(str)

    # Create a new column with famine names that combines dates and simplified places
    tb["famine_name"] = tb["simplified_place"] + " " + tb["date_range"]

    # Rename the cause from natural calamity to climatic adversity (as suggested by the source)
    tb["principal_cause"] = tb["principal_cause"].str.replace("Natural calamity", "Adverse climate")

    # Add origins metadata to new columns.
    for col in [
        "wpf_authoritative_mortality_estimate",
        "famine_name",
    ]:
        tb[col].metadata.origins = tb["simplified_place"].metadata.origins

    # Drop columns that are not needed.
    tb = tb.drop(columns=["date_list", "date_range", "simplified_place"])
    tb = tb.format(["famine_name", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regions(tb: Table, ds_regions: Dataset) -> Table:
    """
    Add regions to the famine data table.
    """
    # First assign custom regions
    tb["region"] = tb["simplified_place"].map(CUSTOM_REGION_DICT)

    # Add the rest as usual
    for region in REGIONS:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(region=region, ds_regions=ds_regions)

        # Add region to the table.
        tb.loc[tb["simplified_place"].isin(countries_in_region), "region"] = region

    return tb
