"""Garden step for Shift data on energy production from fossil fuels.

"""

from pathlib import Path
from typing import List, cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.paths import DATA_DIR

log = get_logger()

NAMESPACE = "shift"
DATASET_SHORT_NAME = "fossil_fuel_production"

VERSION = Path(__file__).parent.name
COUNTRY_MAPPING_PATH = Path(__file__).parent / f"{DATASET_SHORT_NAME}.country_mapping.json"
METADATA_PATH = Path(__file__).parent / f"{DATASET_SHORT_NAME}.meta.yml"

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION = {
    "Czechoslovakia": {
        "continent": "Europe",
        "income_group": "High-income countries",
        "members": [
            # Europe - High-income countries.
            "Czechia",
            "Slovakia",
        ],
    },
    "Netherlands Antilles": {
        "continent": "North America",
        "income_group": "High-income countries",
        "members": [
            # North America - High-income countries.
            "Aruba",
            "Curacao",
            "Sint Maarten (Dutch part)",
        ],
    },
    "USSR": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Lithuania",
            "Estonia",
            "Latvia",
            # Europe - Upper-middle-income countries.
            "Moldova",
            "Belarus",
            "Russia",
            # Europe - Lower-middle-income countries.
            "Ukraine",
            # Asia - Upper-middle-income countries.
            "Georgia",
            "Armenia",
            "Azerbaijan",
            "Turkmenistan",
            "Kazakhstan",
            # Asia - Lower-middle-income countries.
            "Kyrgyzstan",
            "Uzbekistan",
            "Tajikistan",
        ],
    },
    "Yugoslavia": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Croatia",
            "Slovenia",
            # Europe - Upper-middle-income countries.
            "North Macedonia",
            "Bosnia and Herzegovina",
            "Serbia",
            "Montenegro",
        ],
    },
}


def load_income_groups() -> pd.DataFrame:
    """Load dataset of income groups and add historical regions to it.

    Returns
    -------
    income_groups : pd.DataFrame
        Income groups data.

    """
    # Load the WorldBank dataset for income grups.
    income_groups = catalog.Dataset(DATA_DIR / "garden/wb/2021-07-01/wb_income")["wb_income_group"].reset_index()

    # Add historical regions to income groups.
    for historic_region in HISTORIC_TO_CURRENT_REGION:
        historic_region_income_group = HISTORIC_TO_CURRENT_REGION[historic_region]["income_group"]
        if historic_region not in income_groups["country"]:
            historic_region_df = pd.DataFrame(
                {
                    "country": [historic_region],
                    "income_group": [historic_region_income_group],
                }
            )
            income_groups = pd.concat([income_groups, historic_region_df], ignore_index=True)

    return cast(pd.DataFrame, income_groups)


def remove_overlapping_data_between_historical_regions_and_successors(
    data_region: pd.DataFrame,
    index_columns: List[str],
    country_column: str,
    year_column: str,
    ignore_zeros: bool = True,
) -> pd.DataFrame:
    """Remove overlapping data between a historical region and any of its successors (if there is any overlap), to avoid
    double-counting those regions when aggregating data.

    Data for historical regions (e.g. USSR) could overlap with data of the successor countries (e.g. Russia). If this
    happens, remove data (on the overlapping years) of the historical country.

    Parameters
    ----------
    data_region : pd.DataFrame
        Data (after selecting the countries of a certain relevant region).
    index_columns : list
        Index columns
    country_column : str
        Name of column for country names.
    year_column : str
        Name of column for year.
    ignore_zeros : bool
        True to ignore zeros when checking if historical regions overlap with their member countries; this means that,
        if a historical region overlaps with a member, but the member only has zeros in the data, this will not be
        considered an overlap.

    Returns
    -------
    data_region : pd.DataFrame
        Data after removing data with overlapping regions.

    """
    data_region = data_region.copy()

    # Select data columns.
    data_columns = [column for column in data_region.columns if column not in index_columns]
    # Select index columns without country column.
    _index_columns = [column for column in index_columns if column != country_column]
    indexes_to_drop = []

    if ignore_zeros:
        overlapping_values_to_ignore = [0]
    else:
        overlapping_values_to_ignore = []

    for historical_region in HISTORIC_TO_CURRENT_REGION:
        # Successors of the current historical region.
        historical_successors = HISTORIC_TO_CURRENT_REGION[historical_region]["members"]
        # Unique combinations of index for which historical region has data.
        historical_region_years = (
            data_region[(data_region[country_column] == historical_region)]
            .replace(overlapping_values_to_ignore, np.nan)
            .dropna(subset=data_columns, how="all")[_index_columns]
            .dropna()
            .drop_duplicates()
        )
        # Unique combinations of index for which successors have data.
        historical_successors_years = (
            data_region[(data_region[country_column].isin(historical_successors))]
            .replace(overlapping_values_to_ignore, np.nan)
            .dropna(subset=data_columns, how="all")[_index_columns]
            .dropna()
            .drop_duplicates()
        )

        # Find unique years where the above combinations of region and successors overlap.
        overlapping_years = pd.concat([historical_region_years, historical_successors_years], ignore_index=True)
        overlapping_years = overlapping_years[overlapping_years.duplicated()]
        if not overlapping_years.empty:
            log.warning(
                f"Removing rows where historical region {historical_region} overlaps with its successors "
                f"(years {sorted(set(overlapping_years[year_column]))})."
            )
            # Select rows in data_region to drop.
            overlapping_years[country_column] = historical_region
            indexes_to_drop.extend(
                pd.merge(
                    data_region.reset_index(),
                    overlapping_years,
                    how="inner",
                    on=[country_column] + _index_columns,
                )["index"].tolist()
            )

    if len(indexes_to_drop) > 0:
        # Remove rows of data of the historical region where its data overlaps with data from its successors.
        data_region = data_region.drop(index=indexes_to_drop)

    return data_region


def add_region_aggregates(
    data: pd.DataFrame,
    index_columns: List[str],
    country_column: str = "country",
    year_column: str = "year",
) -> pd.DataFrame:
    """Add region aggregate for all regions.

    Regions are defined above, in REGIONS_TO_ADD.

    Parameters
    ----------
    data : pd.DataFrame
        Data.
    index_columns : list
        Index columns
    country_column : str
        Name of country column.
    year_column : str
        Name of year column.

    Returns
    -------
    data : pd.DataFrame
        Data after adding regions.

    """
    data = data.copy()

    income_groups = load_income_groups()
    aggregates = {column: "sum" for column in data.columns if column not in index_columns}
    for region in REGIONS_TO_ADD:
        countries_in_region = geo.list_countries_in_region(region=region, income_groups=income_groups)
        data_region = data[data[country_column].isin(countries_in_region)]

        data_region = remove_overlapping_data_between_historical_regions_and_successors(
            data_region=data_region,
            index_columns=index_columns,
            country_column=country_column,
            year_column=year_column,
        )

        data_region = geo.add_region_aggregates(
            df=data_region,
            region=region,
            country_col=country_column,
            year_col=year_column,
            aggregations=aggregates,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=None,
            num_allowed_nans_per_year=None,
        )
        data = pd.concat([data, data_region[data_region["country"] == region]], ignore_index=True).reset_index(
            drop=True
        )

    return data


def split_ussr_and_russia(df: pd.DataFrame) -> pd.DataFrame:
    """Split data for USSR & Russia into two separate entities (given that Shift treats them as the same entity).

    Parameters
    ----------
    df : pd.DataFrame
        Shift data after harmonizing country names.

    Returns
    -------
    df : pd.DataFrame
        Shift data after separating data for USSR and Russia as separate entities.

    """
    df = df.copy()

    # Name that The Shift Data Portal uses for Russia and USSR.
    shift_ussr_russia_name = "Russian Federation & USSR (Shift)"
    # The relevant part of the data is originally from EIA, who have the first data point for Russia in 1992.
    # Therefore we use this year to split USSR and Russia.
    russia_start_year = 1992
    # Filter to select rows of USSR & Russia data.
    ussr_russia_filter = df["country"] == shift_ussr_russia_name
    ussr_data = (
        df[ussr_russia_filter & (df["year"] < russia_start_year)]
        .replace({shift_ussr_russia_name: "USSR"})
        .reset_index(drop=True)
    )
    russia_data = (
        df[ussr_russia_filter & (df["year"] >= russia_start_year)]
        .replace({shift_ussr_russia_name: "Russia"})
        .reset_index(drop=True)
    )
    # Remove rows where Russia and USSR are combined.
    df = df[~ussr_russia_filter].reset_index(drop=True)
    # Combine original data (without USSR and Russia as one entity) with USSR and Russia as separate entities.
    df = (
        pd.concat([df, ussr_data, russia_data], ignore_index=True)
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    return df


def correct_historical_regions(data: pd.DataFrame) -> pd.DataFrame:
    """Correct some issues in Shift data involving historical regions.

    Parameters
    ----------
    data : pd.DataFrame
        Shift data after harmonization of country names.

    Returns
    -------
    data : pd.DataFrame
        Shift data after doing some corrections related to historical regions.

    """
    data = data.copy()

    # For coal and oil, Czechoslovakia's data become Czechia and Slovakia in 1993.
    # However, for gas, Czechia appear at an earlier date.
    # We correct those rows to be part of Czechoslovakia.
    data_to_add = pd.merge(
        data[(data["year"] < 1980) & (data["country"] == "Czechoslovakia")]
        .reset_index(drop=True)
        .drop(columns=["gas"]),
        data[(data["year"] < 1980) & (data["country"] == "Czechia")].reset_index(drop=True)[["year", "gas"]],
        how="left",
        on="year",
    )
    select_rows_to_correct = (data["country"].isin(["Czechia", "Czechoslovakia"])) & (data["year"] < 1980)
    data = (
        pd.concat([data[~select_rows_to_correct], data_to_add], ignore_index=True)
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    return data


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")
    #
    # Load data.
    #
    # Load meadow dataset and get the only table inside (with the same name).
    ds_meadow = catalog.Dataset(DATA_DIR / f"meadow/{NAMESPACE}/{VERSION}/{DATASET_SHORT_NAME}")
    tb_meadow = ds_meadow[DATASET_SHORT_NAME]

    # Convert table into a dataframe.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Harmonize country names.
    log.info(f"{DATASET_SHORT_NAME}.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=str(COUNTRY_MAPPING_PATH))

    # Remove rows that only have nans.
    df = df.dropna(subset=["coal", "oil", "gas"], how="all").reset_index(drop=True)

    # Treat USSR and Russia as separate entities.
    df = split_ussr_and_russia(df=df)

    # Correct gas data where Czechia and Czechoslovakia overlap.
    df = correct_historical_regions(data=df)

    # Create aggregate regions.
    log.info(f"{DATASET_SHORT_NAME}.add_region_aggregates")
    df = add_region_aggregates(
        data=df,
        index_columns=["country", "year"],
        country_column="country",
        year_column="year",
    )

    # Prepare output data.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset (with the same metadata as the meadow version).
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.save()
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")

    # Create a new table.
    tb_garden = underscore_table(catalog.Table(df))
    tb_garden.metadata = tb_meadow.metadata
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
    # Add table to dataset.
    ds_garden.add(tb_garden)

    log.info(f"{DATASET_SHORT_NAME}.end")
