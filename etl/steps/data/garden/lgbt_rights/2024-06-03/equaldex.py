"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table, VariableMeta, VariablePresentationMeta
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2

# Create a new list of id for each issue, ordered from more liberal to more restrictive (useful for grapher charts)
CATEGORIES_RENAMING = {
    "homosexuality": {
        "Legal": "Legal",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Male illegal, female legal": "Male illegal, female legal or uncertain",
        "Male illegal, female uncertain": "Male illegal, female legal or uncertain",
        "Illegal (other penalty)": "Illegal, prison or other penalty",
        "Illegal (imprisonment as punishment)": "Illegal, prison or other penalty",
        "Illegal (up to life in prison as punishment)": "Illegal, prison or other penalty",
        "Illegal (death penalty as punishment)": "Illegal, death penalty",
    },
    "changing_gender": {
        "Legal, no restrictions": "Legal, no restrictions",
        "Legal, but requires medical diagnosis": "Legal, medical diagnosis required",
        "Legal, but requires surgery": "Legal, surgery required",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Illegal": "Illegal",
    },
    "marriage": {
        "Legal": "Legal",
        "Civil unions (limited rights)": "Civil union or other partnership",
        "Civil unions (marriage rights)": "Civil union or other partnership",
        "Other type of partnership": "Civil union or other partnership",
        "Foreign same-sex marriages recognized only": "Foreign same-sex marriages recognized only",
        "Unregistered cohabitation": "Unregistered cohabitation",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Unrecognized": "Unrecognized",
        "Banned": "Banned",
    },
    "adoption": {
        "Legal": "Legal",
        "Married couples only": "Married couples only",
        "Second parent adoption only": "Second parent adoption only",
        "Varies by Region": "Varies by region",
        "Single only": "Single only",
        "Ambiguous": "Ambiguous",
        "Illegal": "Illegal",
    },
    "age_of_consent": {
        "Equal": "Equal",
        "Female equal, male N/A": "Female equal, male illegal",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Unequal": "Unequal",
    },
    "blood": {
        "Legal": "Legal",
        "Legal with restrictions": "Legal with restrictions",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Banned (less than 6-month deferral)": "Banned (less than 6-month deferral)",
        "Banned (6-month deferral)": "Banned (6-month deferral)",
        "Banned (1-year deferral)": "Banned (1-year deferral)",
        "Banned (5-year deferral)": "Banned (5-year deferral)",
        "Banned (indefinite deferral)": "Banned (indefinite deferral)",
    },
    "censorship": {
        "No censorship": "No censorship",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Other punishment": "Other punishment",
        "Fine as punishment": "Fine as punishment",
        "State-enforced": "State-enforced",
        "Imprisonment as punishment": "Imprisonment as punishment",
    },
    "conversion_therapy": {
        "Banned": "Banned",
        "Varies by Region": "Varies by region",
        "Sexual orientation only": "Sexual orientation only",
        "Ambiguous": "Ambiguous",
        "Not banned": "Not banned",
    },
    "discrimination": {
        "Illegal": "Illegal",
        "Illegal in some contexts": "Illegal in some contexts",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "No protections": "No protections",
    },
    "employment_discrimination": {
        "Sexual orientation and gender identity": "Sexual orientation and gender identity",
        "Gender identity only": "Gender identity only",
        "Sexual orientation only": "Sexual orientation only",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "No protections": "No protections",
    },
    "housing_discrimination": {
        "Sexual orientation and gender identity": "Sexual orientation and gender identity",
        "Gender identity only": "Gender identity only",
        "Sexual orientation only": "Sexual orientation only",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "No protections": "No protections",
    },
    "military": {
        "Legal": "Legal",
        "Don't Ask, Don't Tell": "\"Don't ask, don't tell\"",
        "Lesbians, gays, bisexuals permitted, transgender people banned": "LGB permitted, transgender people banned",
        "Ambiguous": "Ambiguous",
        "Illegal": "Illegal",
    },
    "non_binary_gender_recognition": {
        "Recognized": "Recognized",
        "Intersex only": "Intersex only",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Not legally recognized": "Not legally recognized",
    },
    "gender_affirming_care": {
        "Legal": "Legal",
        "Legal, but restricted for minors": "Legal, but restricted for minors",
        "Legal, but banned for minors": "Legal, but banned for minors",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Restricted": "Restricted",
        "Banned": "Banned",
    },
    "intersex_infant_surgery": {
        "Full ban": "Full ban",
        "Parental approval required": "Parental approval required",
        "Varies by Region": "Varies by region",
        "Ambiguous": "Ambiguous",
        "Not banned": "Not banned",
    },
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow and regions and population datasets.
    ds_meadow = paths.load_dataset("equaldex")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")
    ds_sovereign_countries = paths.load_dataset("isd")

    # Read tables from meadow dataset.
    tb = ds_meadow["equaldex"].reset_index()
    tb_current = ds_meadow["equaldex_current"].reset_index()
    tb_indices = ds_meadow["equaldex_indices"].reset_index()
    tb_sovereign_countries = ds_sovereign_countries["isd_countries"].reset_index()

    #
    # Process data.

    tb = make_table_wide_and_map_categories(tb)
    tb_current = make_table_wide_and_map_categories(tb_current)

    # Merge both datasets and include the suffix _current to the columns of the current dataset
    tb = pr.merge(tb, tb_current, on=["country", "year"], how="outer", suffixes=("", "_current"))

    # Merge table with indices
    tb = pr.merge(tb, tb_indices, on=["country", "year"], how="left")

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Select only sovereign countries
    tb = select_only_sovereign_countries(tb=tb, tb_sovereign_countries=tb_sovereign_countries)

    # Add population-weighted aggregations for the columns in the list
    tb = add_population_weighted_aggregations(
        tb=tb,
        columns=["ei", "ei_legal", "ei_po"],
        ds_regions=ds_regions,
        ds_population=ds_population,
        regions=REGIONS,
    )

    # Add country counts and population by status for the columns in the list
    tb = add_country_counts_and_population_by_status(
        tb=tb,
        columns=list(CATEGORIES_RENAMING.keys()),
        ds_regions=ds_regions,
        ds_population=ds_population,
        regions=REGIONS,
    )

    # Verify index and order them
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_wide_and_map_categories(tb: Table) -> Table:
    """
    Make the reable wide by pivoting on the issue column and map the categories to clearer names, sorted by order of progressiveness.
    """
    # Make value_formatted a string
    tb["value_formatted"] = tb["value_formatted"].astype("string")

    # Make the dataframe wide by pivoting on the issue column.
    tb = tb.pivot(
        index=["country", "year"], columns="issue", values=["value_formatted"], join_column_levels_with="_"
    ).reset_index(drop=True)

    # replace all column names with - with _
    tb.columns = tb.columns.str.replace("-", "_")

    # Remove "value_formatted_"
    tb.columns = tb.columns.str.removeprefix("value_formatted_")

    # Define issues
    issue_list = list(CATEGORIES_RENAMING.keys())

    # Rename categories
    for issue in issue_list:
        tb[issue] = map_series(
            series=tb[issue],
            mapping=CATEGORIES_RENAMING[issue],
            warn_on_missing_mappings=False,
            warn_on_unused_mappings=False,
            show_full_warning=False,
        )
        tb[issue] = tb[issue].copy_metadata(tb["country"])

        # Assign an order to the categories, by modifying the metadata of the column
        tb[issue].m.type = "ordinal"
        # Create a list of unique categories and remove duplicates
        categories_from_dict = list(dict.fromkeys(CATEGORIES_RENAMING[issue].values()))
        categories_from_column = list(tb[issue].dropna().unique().copy())

        # Remove values in categories_from_dict that are not in categories_from_column
        categories_list = [x for x in categories_from_dict if x in categories_from_column]
        tb[issue].m.sort = categories_list

    # Assert if the issues available are EXPECTED_COLUMNS
    assert set(issue_list + ["country", "year"]) == set(tb.columns), paths.log.error(
        f"There are more columns than the ones expected: {set(tb.columns).difference(set(issue_list + ['country', 'year']))}"
    )

    # Keep only the columns we need
    tb = tb[["country", "year"] + issue_list]

    return tb


def add_population_weighted_aggregations(
    tb: Table, columns: List[str], ds_regions: Dataset, ds_population: Dataset, regions: List[str]
) -> Table:
    """
    Add population-weighted aggregations for the columns in the list
    """

    tb = tb.copy()

    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

    columns_pop = []
    for col in columns:
        tb[f"{col}_pop"] = tb[col] * tb["population"]
        columns_pop.append(f"{col}_pop")

    aggregations = dict.fromkeys(
        columns_pop + ["population"],
        "sum",
    )

    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Estimate population-weighted aggregations
    for col in columns:
        tb[f"{col}"] = tb[f"{col}_pop"] / tb["population"]

    # Drop columns we don't need anymore
    tb = tb.drop(columns=columns_pop + ["population"])

    return tb


def add_country_counts_and_population_by_status(
    tb: Table, columns: List[str], ds_regions: Dataset, ds_population: Dataset, regions: List[str]
) -> Table:
    """
    Add country counts and population by status for the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Define empty dictionaries for each of the columns
    columns_count_dict = {columns[i]: [] for i in range(len(columns))}
    columns_pop_dict = {columns[i]: [] for i in range(len(columns))}
    for col in columns:
        # Fill nan values with "missing"
        tb_regions[col] = tb_regions[col].fillna("missing")
        # Get the unique values in the column
        status_list = list(tb_regions[col].unique())
        for status in status_list:
            # Calculate count and population for each status in the column
            tb_regions[f"{col}_{status}_count"] = tb_regions[col].apply(lambda x: 1 if x == status else 0)
            tb_regions[f"{col}_{status}_pop"] = tb_regions[f"{col}_{status}_count"] * tb_regions["population"]

            # Add metadata for the new columns
            tb_regions[f"{col}_{status}_count"].metadata = add_metadata_for_aggregated_columns(
                col=col, status=status, count_or_pop="count", origins=tb_regions[f"{col}_{status}_count"].m.origins
            )
            tb_regions[f"{col}_{status}_pop"].metadata = add_metadata_for_aggregated_columns(
                col=col, status=status, count_or_pop="pop", origins=tb_regions[f"{col}_{status}_pop"].m.origins
            )

            # Add the new columns to the list
            columns_count_dict[col].append(f"{col}_{status}_count")
            columns_pop_dict[col].append(f"{col}_{status}_pop")

    # Create a new list with all the count columns and population columns
    columns_count = [item for sublist in columns_count_dict.values() for item in sublist]
    columns_pop = [item for sublist in columns_pop_dict.values() for item in sublist]

    aggregations = dict.fromkeys(
        columns_count + columns_pop + ["population"],
        "sum",
    )

    tb_regions = geo.add_regions_to_table(
        tb=tb_regions,
        ds_regions=ds_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Remove population column
    tb_regions = tb_regions.drop(columns=["population"])

    # Add population again
    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Calculate the missing population for each region
    for col in columns:
        # Calculate the missing population for each column, by subtracting the population of the countries with data from the total population
        tb_regions[f"{col}_missing_pop_other_countries"] = tb_regions["population"] - tb_regions[
            columns_pop_dict[col]
        ].sum(axis=1)
        tb_regions[f"{col}_missing_pop"] = (
            tb_regions[f"{col}_missing_pop"] + tb_regions[f"{col}_missing_pop_other_countries"]
        )

        tb_regions[f"{col}_missing_pop"].metadata = add_metadata_for_aggregated_columns(
            col=col, status="missing", count_or_pop="pop", origins=tb_regions[f"{col}_missing_pop"].m.origins
        )

    # Keep only the regions in the country column
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].copy().reset_index(drop=True)

    # Keep only the columns I need
    tb_regions = tb_regions[["country", "year"] + columns_count + columns_pop]

    # Merge the two tables
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="left")

    return tb


def add_metadata_for_aggregated_columns(col: str, status: str, count_or_pop: str, origins) -> VariableMeta:
    if count_or_pop == "count":
        meta = VariableMeta(
            title=f"{col.capitalize()} - {status.capitalize()} (Count)",
            description_short=f"Number of countries with the status '{status}' for {col}.",
            unit="countries",
            short_unit="",
            sort=[],
            origins=origins,
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": 0,
            "tolerance": 0,
        }
        meta.presentation = VariablePresentationMeta(title_public=meta.title)
    elif count_or_pop == "pop":
        meta = VariableMeta(
            title=f"{col.capitalize()} - {status.capitalize()} (Population)",
            description_short=f"Population of countries with the status '{status}' for {col}.",
            unit="persons",
            short_unit="",
            sort=[],
            origins=origins,
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": 0,
            "tolerance": 0,
        }
        meta.presentation = VariablePresentationMeta(title_public=meta.title)

    else:
        paths.log.error(f"count_or_pop must be either 'count' or 'pop'. Got {count_or_pop}.")

    return meta  # type: ignore


def select_only_sovereign_countries(tb: Table, tb_sovereign_countries: Table) -> Table:
    """
    Use the latest sovereign countries data to select only those countries in the table
    """

    # Format tb_sovereign_countries
    # Rename regions to country
    tb_sovereign_countries = tb_sovereign_countries.rename({"statename": "country"})
    tb_sovereign_countries = tb_sovereign_countries[["country", "year"]]

    # Filter data: max year
    tb_sovereign_countries = tb_sovereign_countries[
        (tb_sovereign_countries["year"] == tb_sovereign_countries["year"].max())
    ]

    # Drop year column
    tb_sovereign_countries = tb_sovereign_countries.drop(columns=["year"])

    # Merge the two tables
    tb = pr.merge(tb, tb_sovereign_countries, on=["country"], how="inner")

    return tb
