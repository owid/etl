"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from owid.catalog.utils import underscore

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("homicide")
    # Load full population dataset including projections to calculate rates
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
    tb = paths.regions.harmonize_names(tb)

    sanity_check_inputs(tb)
    tb = clean_up_categories(tb)
    tb = calculate_united_kingdom(tb)
    # Calculate rates for any country-year with counts but no source-provided rate
    tb = calculate_rates_for_missing_years(tb, tb_pop_full)
    # Clean up variable names.
    tb = clean_up_variables(tb)
    tables = clean_data(tb)
    sanity_check_outputs(tables)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.empty, "Meadow table is empty."
    expected_dims = {"Total", "by mechanisms", "by relationship to perpetrator", "by situational context"}
    assert expected_dims == set(tb["dimension"].unique()), f"Unexpected dimensions: {set(tb['dimension'].unique())}"
    assert tb["year"].min() <= 1990, f"Expected data back to at least 1990, got {tb['year'].min()}"
    assert tb["year"].max() >= 2023, f"Expected data up to at least 2023, got {tb['year'].max()}"
    assert tb["country"].nunique() >= 200, f"Expected at least 200 countries, got {tb['country'].nunique()}"


def sanity_check_outputs(tables: list[Table]) -> None:
    assert len(tables) == 4, f"Expected 4 output tables, got {len(tables)}"
    for t in tables:
        assert not t.empty, f"Output table {t.metadata.short_name} is empty."
        assert t.columns[t.isna().all()].empty, f"Output table {t.metadata.short_name} has a fully-NaN column."


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


def calculate_rates_for_missing_years(tb: Table, tb_pop_full: Table) -> Table:
    """
    Calculate rates for any country-year that has counts but no source-provided rate,
    using counts from UNODC and population from UN WPP.
    """
    key_cols = ["country", "year", "indicator", "dimension", "category", "sex", "age"]

    tb_counts = tb[tb["unit_of_measurement"] == "Counts"]
    tb_rates = tb[tb["unit_of_measurement"] == "Rate per 100,000 population"]

    if tb_counts.empty:
        return tb

    # Find (key) combos that have counts but no rate
    counts_keys = tb_counts[key_cols].drop_duplicates()
    rates_keys = tb_rates[key_cols].drop_duplicates()
    missing = pr.merge(counts_keys, rates_keys, on=key_cols, how="left", indicator=True)
    missing = missing[missing["_merge"] == "left_only"].drop(columns=["_merge"])

    if missing.empty:
        return tb

    # Keep only the count rows that need a rate
    tb_need_rate = pr.merge(tb_counts, missing, on=key_cols, how="inner")

    # Merge with population to compute rate
    tb_merged = pr.merge(tb_need_rate, tb_pop_full, on=["country", "year", "sex"], how="inner")
    tb_merged["value"] = tb_merged["value"] / tb_merged["population"] * 100000
    tb_merged["unit_of_measurement"] = "Rate per 100,000 population"
    tb_merged = tb_merged.drop(columns=["population"])

    tb = pr.concat([tb, tb_merged])
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

    for key in category_dict:
        assert key in tb["category"].values, f"{key} not in table"
    tb["category"] = tb["category"].replace(category_dict)

    assert tb["category"].isna().sum() == 0
    return tb


def calculate_united_kingdom(tb: Table) -> Table:
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
    tb_uk_rate = paths.regions.add_population(tb_uk_rate)
    tb_uk_rate["value"] = tb_uk_rate["value"] / tb_uk_rate["population"] * 100000
    tb_uk_rate["unit_of_measurement"] = "Rate per 100,000 population"
    tb_uk_rate = tb_uk_rate.drop(columns=["population"])

    tb = pr.concat([tb, tb_uk, tb_uk_rate])
    return tb
