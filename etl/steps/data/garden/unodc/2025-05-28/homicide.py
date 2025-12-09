"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("homicide")
    # Load population dataset only goes up to 2023
    ds_population = paths.load_dataset("population")
    # Load full population dataset including projections to calculate rates for 2024
    ds_pop_full = paths.load_dataset("un_wpp")
    tb_pop_full = ds_pop_full.read("population")
    # Format the population table
    tb_pop_full = format_pop_full(tb_pop_full)
    # Read table from meadow dataset.
    tb = ds_meadow.read("homicide")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = clean_up_categories(tb)
    tb = calculate_united_kingdom(tb, ds_population)
    # Calculate rates for 2024 using counts from UNODC and medium population projections from UN WPP
    tb = calculate_rates_for_most_recent_year(tb, tb_pop_full)
    # Clean up variable names.
    tb = clean_up_variables(tb)
    tables = clean_data(tb)
    # Improve table format.

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def clean_up_variables(tb: Table) -> Table:
    """
    Clean up the variable names in the table by making the names a bit more readable.
    """
    category_dict = {
        "Firearms or explosives - firearms": "a firearm",
        "sharp or blunt object, including motor vehicles": "a sharp or blunt object, including a motor vehicle",
        "firearms or explosives": "a firearm or explosive",
    }
    tb["category"] = tb["category"].replace(category_dict)

    return tb


def format_pop_full(tb_pop_full: Table) -> Table:
    tb_pop_full = tb_pop_full[(tb_pop_full["variant"] == "medium") & (tb_pop_full["age"] == "all")]
    tb_pop_full = tb_pop_full.drop(columns=["variant", "age", "population_change", "population_density"])
    tb_pop_full = tb_pop_full.replace({"male": "Male", "female": "Female", "all": "Total"})
    return tb_pop_full


def calculate_rates_for_most_recent_year(tb: Table, tb_pop_full: Table) -> Table:
    """
    Calculate rates for the most recent year in the dataset using the full population dataset, with projections included.
    """
    # Get the most recent year in the table for counts
    tb_counts = tb[tb["unit_of_measurement"] == "Counts"]
    tb_rates = tb[tb["unit_of_measurement"] == "Rate per 100,000 population"]
    if tb_rates.empty:
        most_recent_year_rates = None
        print("No rates found in the table, check spelling.")
    else:
        most_recent_year_rates = tb_rates["year"].max()
    most_recent_year_counts = tb_counts["year"].max()

    if most_recent_year_rates is not None and most_recent_year_counts > most_recent_year_rates:
        tb_recent = tb_counts[tb_counts["year"] == most_recent_year_counts]
        tb_pop_recent = tb_pop_full[tb_pop_full["year"] == most_recent_year_counts]
        tb_merge = pr.merge(left=tb_recent, right=tb_pop_recent, on=["country", "year", "sex"])
        tb_merge["value_new"] = tb_merge["value"] / tb_merge["population"] * 100000

        tb_merge["unit_of_measurement"] = "Rate per 100,000 population"
        tb_merge = tb_merge.drop(columns=["population", "value"])

        tb_merge = tb_merge.rename(columns={"value_new": "value"})
        tb = pr.concat([tb, tb_merge])

    return tb


def clean_data(tb: Table) -> list[Table]:
    """
    Splitting the data into four dataframes/tables based on the dimension columns:
    * Total
    * by mechanism
    * by relationship to perpetrator
    * by situational context
    """
    tb_mech = create_table(tb, table_name="by mechanisms")
    tb_perp = create_table(tb, table_name="by relationship to perpetrator")
    tb_situ = create_table(tb, table_name="by situational context")
    tb_tot = create_total_table(tb)

    tb_garden_list = [tb_mech, tb_tot, tb_perp, tb_situ]

    return tb_garden_list


def create_table(tb: Table, table_name: str) -> Table:
    """
    Create the homicides by mechanism dataframe where we will have  homicides/homicide rate
    disaggregated by mechanism (e.g. weapon)

    """
    assert any(tb["dimension"] == table_name), "table_name must be a dimension in df"
    tb_filter = tb[tb["dimension"] == table_name]
    tb_filter = tb_filter.drop(columns=["indicator", "source", "dimension"])

    tb_filter = tb_filter.format(
        ["country", "year", "category", "sex", "age", "unit_of_measurement"],
        short_name=underscore(table_name),
    )

    return tb_filter


def create_total_table(tb: Table) -> Table:
    """
    Create the total homicides dataframe where we will have total homicides/homicide rate
    disaggregated by age and sex
    """
    tb_tot = tb[tb["dimension"] == "Total"]

    # There are some duplicates when sex is unknown so let's remove those rows
    tb_tot = tb_tot[tb_tot["sex"] != "Unknown"]

    tb_tot = tb_tot.drop(columns=["indicator", "source", "dimension"])

    tb_tot = tb_tot.format(
        ["country", "year", "category", "sex", "age", "unit_of_measurement"],
        short_name="total",
    )

    return tb_tot


def clean_up_categories(tb: Table) -> Table:
    """
    Make the categories used in the dataset a bit more readable.

    """
    category_dict = {
        "Another weapon - sharp object": "a sharp object",
        "Unspecified means": "unspecified means",
        "Without a weapon/ other Mechanism": "without a weapon or by another mechanism",
        "Firearms or explosives": "firearms or explosives",
        "Another weapon": "sharp or blunt object, including motor vehicles",
        "Intimate partner or family member": "Perpetrator is an intimate partner or family member of the victim",
        "Intimate partner or family member: Intimate partner": "Perpetrator is an intimate partner",
        "Intimate partner or family member: Family member": "Perpetrator is a family member",
        "Other Perpetrator known to the victim": "Another known perpetrator",
        "Perpetrator unknown to the victim": "Perpetrator is unknown to victim",
        "Perpetrator to victim relationship unknown": "the relationship to the victim is not known",
        "Socio-political homicide - terrorist offences": "Terrorist offences",
        "Unknown types of homicide": "Unknown situational context",
    }

    for key in category_dict.keys():
        assert key in tb["category"].values, f"{key} not in table"
    tb["category"] = tb["category"].replace(category_dict)

    assert tb["category"].isna().sum() == 0
    return tb


def calculate_united_kingdom(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate data for the UK as it is reported by the constituent countries
    """

    countries = ["England and Wales", "Scotland", "Northern Ireland"]
    tb_uk = tb[(tb["country"].isin(countries)) & (tb["unit_of_measurement"] == "Counts")]

    tb_uk = (
        tb_uk.groupby(["year", "indicator", "dimension", "category", "sex", "age", "unit_of_measurement"])
        .agg(value=("value", "sum"), count=("value", "size"))
        .reset_index()
    )
    # Use only rows where all three entities are in the data
    tb_uk = tb_uk[tb_uk["count"] == 3]
    tb_uk["country"] = "United Kingdom"
    tb_uk = tb_uk.drop(columns="count")

    # Add in UK population to calculate rates
    tb_uk_rate = tb_uk.copy()
    tb_uk_rate = add_population_to_table(tb_uk_rate, ds_population)
    tb_uk_rate["value"] = tb_uk_rate["value"] / tb_uk_rate["population"] * 100000
    tb_uk_rate["unit_of_measurement"] = "Rate per 100,000 population"
    tb_uk_rate = tb_uk_rate.drop(columns=["population"])

    tb = pr.concat([tb, tb_uk, tb_uk_rate])
    return tb
