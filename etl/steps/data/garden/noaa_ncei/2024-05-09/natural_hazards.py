"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from owid.datautils import dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# The common impact metrics among the three tables are as follows:
# Note that the definitions are not easy to be found explicitly anywhere.
# But in the resulting search table, by clicking on the header of each column, a pop up appears with a short definition.
# https://www.ngdc.noaa.gov/hazel/view/hazards/tsunami/event-data
# https://www.ngdc.noaa.gov/hazel/view/hazards/earthquake/event-data
# https://www.ngdc.noaa.gov/hazel/view/hazards/volcano/event-data
# I manually extracted and adapted those definitions for the main metrics below.
COLUMNS = {
    # Keep column "id" for two reasons. Firstly, to detect repeated events (I have seen at least one). Secondly, to
    # count the number of events per country-year.
    "id": "id",
    "country": "country",
    "year": "year",
    # Whenever possible, numbers of deaths are listed.
    "deaths": "deaths",
    # When a description was found in the historical literature instead of an actual number of deaths, this value was coded and listed in the Deaths column. If the actual number of deaths was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 deaths)
    # 2	Some (~51 to 100 deaths)
    # 3	Many (~101 to 1000 deaths)
    # 4	Very many (over 1000 deaths)
    # "deathsamountorder": "deaths_estimate",
    # Whenever possible, numbers of missing are listed.
    "missing": "missing",
    # When a description was found in the historical literature instead of an actual number of missing, this value was coded and listed in the Missing column. If the actual number of missing was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 missing)
    # 2	Some(~51 to 100 missing)
    # 3	Many (~101 to 1000 missing)
    # 4	Very Many (~1001 or more missing)
    # "missingamountorder": "missing_estimate",
    # Whenever possible, numbers of injuries from the disaster are listed.
    "injuries": "injuries",
    # When a description was found in the historical literature instead of an actual number of injuries, this value was coded and listed in the Injuries column. If the actual number of injuries was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 injuries)
    # 2	Some(~51 to 100 injuries)
    # 3	Many (~101 to 1000 injuries)
    # 4	Very many (over 1000 injuries)
    # "injuriesamountorder": "injuries_estimate",
    # The value in the Damage column should be multiplied by 1,000,000 to obtain the actual dollar amount.
    # When a dollar amount for damage was found in the literature, it was listed in the Damage column in millions of U.S. dollars. The dollar value listed is the value at the time of the event. To convert the damage to current dollar values, please use the Consumer Price Index Calculator. Monetary conversion tables for the time of the event were used to convert foreign currency to U.S. dollars.
    "damagemillionsdollars": "damage",
    # For those events not offering a monetary evaluation of damage, the following five-level scale was used to classify damage (1990 dollars) and was listed in the Damage column. If the actual dollar amount of damage was listed, a descriptor was also added for search purposes.
    # 0	NONE
    # 1	LIMITED (roughly corresponding to less than $1 million)
    # 2	MODERATE (~$1 to $5 million)
    # 3	SEVERE (~$5 to $25 million)
    # 4	EXTREME (~$25 million or more)
    # When possible, a rough estimate was made of the dollar amount of damage based upon the description provided, in order to choose the damage category. In many cases, only a single descriptive term was available. These terms were converted to the damage categories based upon the authors apparent use of the term elsewhere. In the absence of other information, LIMITED is considered synonymous with slight, minor, and light, SEVERE as synonymous with major, extensive, and heavy, and EXTREME as synonymous with catastrophic.
    # Note: The descriptive terms relate approximately to current dollar values.
    # "damageamountorder": "damage_estimate",
    # Whenever possible, numbers of houses destroyed are listed.
    "housesdestroyed": "houses_destroyed",
    # For those events not offering an exact number of houses damaged, the following four-level scale was used to classify the damage and was listed in the Houses Destroyed column. If the actual number of houses destroyed was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 houses)
    # 2	Some (~51 to 100 houses)
    # 3	Many (101 to 1000 houses)
    # 4	Very Many (~over 1000 houses)
    # "housesdestroyedamountorder": "houses_destroyed_estimate",
    # Whenever possible, numbers of houses damaged are listed.
    "housesdamaged": "houses_damaged",
    # For those events not offering an exact number of houses damaged, the following four-level scale was used to classify the damage and was listed in the Houses Damaged column. If the actual number of houses destroyed was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 houses)
    # 2	Some (~51 to 100 houses)
    # 3	Many (101 to 1000 houses)
    # 4	Very Many (~over 1000 houses)
    # "housesdamagedamountorder": "houses_damaged_estimate",
    # Whenever possible, total number of deaths from the disaster and secondary effects are listed.
    "deathstotal": "deaths_total",
    # When a description was found in the historical literature instead of an actual number of deaths, this value was coded and listed in the Deaths column. If the actual number of deaths was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 deaths)
    # 2	Some (~51 to 100 deaths)
    # 3	Many (~101 to 1000 deaths)
    # 4	Very many (over 1000 deaths)
    # "deathsamountordertotal": "deaths_total_estimate",
    # Whenever possible, total number of missing from the disaster and secondary effects are listed.
    "missingtotal": "missing_total",
    # When a description was found in the historical literature instead of an actual number of missing, this value was coded and listed in the Missing column. If the actual number of missing was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 missing)
    # 2	Some(~51 to 100 missing)
    # 3	Many (~101 to 1000 missing)
    # 4	Very Many (~1001 or more missing)
    # "missingamountordertotal": "missing_total_estimate",
    # Whenever possible, total number of injuries from the disaster and secondary effects are listed.
    "injuriestotal": "injuries_total",
    # When a description was found in the historical literature instead of an actual number of injuries, this value was coded and listed in the Injuries column. If the actual number of injuries was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 injuries)
    # 2	Some(~51 to 100 injuries)
    # 3	Many (~101 to 1000 injuries)
    # 4	Very many (over 1000 injuries)
    # "injuriesamountordertotal": "injuries_total_estimate",
    # The value in the Damage column should be multiplied by 1,000,000 to obtain the actual dollar amount.
    # When a dollar amount for damage was found in the literature, it was listed in the Damage column in millions of U.S. dollars. The dollar value listed is the value at the time of the event. To convert the damage to current dollar values, please use the Consumer Price Index Calculator. Monetary conversion tables for the time of the event were used to convert foreign currency to U.S. dollars.
    "damagemillionsdollarstotal": "damage_total",
    # For those events not offering a monetary evaluation of damage, the following five-level scale was used to classify damage (1990 dollars) and was listed in the Damage column. If the actual dollar amount of damage was listed, a descriptor was also added for search purposes.
    # 0	NONE
    # 1	LIMITED (roughly corresponding to less than $1 million)
    # 2	MODERATE (~$1 to $5 million)
    # 3	SEVERE (~$5 to $25 million)
    # 4	EXTREME (~$25 million or more)
    # When possible, a rough estimate was made of the dollar amount of damage based upon the description provided, in order to choose the damage category. In many cases, only a single descriptive term was available. These terms were converted to the damage categories based upon the authors apparent use of the term elsewhere. In the absence of other information, LIMITED is considered synonymous with slight, minor, and light, SEVERE as synonymous with major, extensive, and heavy, and EXTREME as synonymous with catastrophic.
    # Note: The descriptive terms relate approximately to current dollar values.
    # "damageamountordertotal": "damage_total_estimate",
    # Whenever possible, numbers of houses destroyed are listed.
    # NOTE: Here I suppose it includes houses destroyed by secondary effects.
    "housesdestroyedtotal": "houses_destroyed_total",
    # For those events not offering an exact number of houses damaged, the following four-level scale was used to classify the damage and was listed in the Houses Destroyed column. If the actual number of houses destroyed was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 houses)
    # 2	Some (~51 to 100 houses)
    # 3	Many (101 to 1000 houses)
    # 4	Very Many (~over 1000 houses)
    # "housesdestroyedamountordertotal": "houses_destroyed_total_estimate",
    # Whenever possible, numbers of houses damaged are listed.
    # NOTE: Here I suppose it includes houses damaged by secondary effects.
    "housesdamagedtotal": "houses_damaged_total",
    # For those events not offering an exact number of houses damaged, the following four-level scale was used to classify the damage and was listed in the Houses Damaged column. If the actual number of houses destroyed was listed, a descriptor was also added for search purposes.
    # 0	None
    # 1	Few (~1 to 50 houses)
    # 2	Some (~51 to 100 houses)
    # 3	Many (101 to 1000 houses)
    # 4	Very Many (~over 1000 houses)
    # "housesdamagedamountordertotal": "houses_damaged_total_estimate",
}


# Imported and adapted from the garden natural_disasters step.
def create_decadal_average(tb: Table) -> Table:
    """Create data of average impacts over periods of 10 years.

    For example (as explained in the footer of the natural disasters explorer), the value for 1900 of any column should
    represent the average of that column between 1900 and 1909.

    """
    tb_decadal = tb.copy()

    # Ensure each country has data for all years (and fill empty rows with zeros).
    # Otherwise, the average would be performed only across years for which we have data.
    # For example, if we have data only for 1931 (and no other year in the 1930s) we want that data point to be averaged
    # over all years in the decade (assuming they are all zero).
    # Note that, for the current decade, since it's not complete, we want to average over the number of current years
    # (not the entire decade).

    # List all countries, years and types in the data.
    countries = sorted(set(tb_decadal["country"]))
    years = np.arange(tb_decadal["year"].min(), tb_decadal["year"].max() + 1).tolist()
    types = sorted(set(tb_decadal["type"]))

    # Create a new index covering all combinations of countries, years and types.
    new_indexes = pd.MultiIndex.from_product([countries, years, types], names=["country", "year", "type"])

    # Reindex data so that all countries and types have data for each year (filling with zeros when there's no data).
    tb_decadal = tb_decadal.set_index(["country", "year", "type"]).reindex(new_indexes, fill_value=0).reset_index()

    # For each year, calculate the corresponding decade (e.g. 1951 -> 1950, 1929 -> 1920).
    tb_decadal["decade"] = (tb_decadal["year"] // 10) * 10

    # Group by that country-decade-type and get the mean for each column.
    tb_decadal = (
        tb_decadal.drop(columns=["year"])
        .groupby(["country", "decade", "type"], observed=True)
        .mean(numeric_only=True)
        .reset_index()
        .rename(columns={"decade": "year"})
    )

    return tb_decadal


def create_decadal_total(tb: Table) -> Table:
    # Alternative approach: Instead of calculating the mean, do the total, respecting the empty values.
    tb_decadal = tb.copy()

    # List all countries, years and types in the data.
    countries = sorted(set(tb_decadal["country"]))
    years = np.arange(tb_decadal["year"].min(), tb_decadal["year"].max() + 1).tolist()
    types = sorted(set(tb_decadal["type"]))

    # Create a new index covering all combinations of countries, years and types.
    new_indexes = pd.MultiIndex.from_product([countries, years, types], names=["country", "year", "type"])

    # Reindex data so that all countries and types have data for each year (filling with zeros when there's no data).
    # NOTE: This time (unlike in the average case) we don't want to fill with zeros, but with NaNs.
    tb_decadal = tb_decadal.set_index(["country", "year", "type"]).reindex(new_indexes, fill_value=None).reset_index()

    # For each year, calculate the corresponding decade (e.g. 1951 -> 1950, 1929 -> 1920).
    tb_decadal["decade"] = (tb_decadal["year"] // 10) * 10

    tb_decadal = tb_decadal.drop(columns=["year"])
    tb_decadal = (
        dataframes.groupby_agg(
            df=tb_decadal,
            groupby_columns=["country", "decade", "type"],
            aggregations={
                column: "sum" for column in tb_decadal.columns if column not in ["country", "decade", "type"]
            },
            min_num_values=1,
        )
        .reset_index()
        .rename(columns={"decade": "year"})
    )

    return tb_decadal


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("natural_hazards")
    tables = {
        "earthquakes": ds_meadow["natural_hazards_earthquakes"].reset_index(),
        "tsunamis": ds_meadow["natural_hazards_tsunamis"].reset_index(),
        "volcanoes": ds_meadow["natural_hazards_volcanoes"].reset_index(),
    }

    # Load regions and income groups datasets.
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Process each table.
    for table_name in tables:
        # Fix known data issues.
        if table_name == "earthquakes":
            error = "Data issue with earthquake event id 10737 (missing country) may have been fixed. Remove this code."
            assert set(tables[table_name][tables[table_name]["country"].isnull()]["id"]) == {10583}, error
            tables[table_name].loc[tables[table_name]["id"] == 10583, "country"] = "GREECE"
        elif table_name == "tsunamis":
            error = "Data issue with earthquake event id 3190 (missing country) may have been fixed. Remove this code."
            assert set(tables[table_name][tables[table_name]["country"].isnull()]["id"]) == {3190}, error
            tables[table_name]["country"] = tables[table_name]["country"].cat.add_categories(["MEDITERRANEAN SEA"])
            tables[table_name].loc[tables[table_name]["id"] == 3190, "country"] = "MEDITERRANEAN SEA"
        else:
            error = f"Unexpected missing country in table {table_name}."
            assert set(tables[table_name][tables[table_name]["country"].isnull()]["id"]) == set(), error

        if "eventvalidity" in tables[table_name].columns:
            # This column exists for tsunamis data, and informs of the validity of the reported event.
            # Meaning (extracted from https://www.ngdc.noaa.gov/hazel/view/hazards/tsunami/event-data):
            # -1 erroneous entry,
            # 0 event that only caused a seiche or disturbance in an inland river/lake
            # 1	very doubtful tsunami
            # 2	questionable tsunami
            # 3	probable tsunami
            # 4	definite tsunami
            # TODO: Confirm that we should select only probable and definite tsunami, and mention it in metadata.
            tables[table_name] = tables[table_name][tables[table_name]["eventvalidity"] >= 3]

        # Select and rename columns.
        columns_available = {column: COLUMNS[column] for column in COLUMNS if column in tables[table_name].columns}
        tables[table_name] = tables[table_name][list(columns_available)].rename(
            columns=columns_available, errors="raise"
        )

        # Drop events with the same "id" within the same disaster type, (we know at least one, id 1926, for Chile 1961).
        tables[table_name] = tables[table_name].drop_duplicates(subset=["id"]).reset_index(drop=True)

        # Harmonize country names.
        tables[table_name] = geo.harmonize_countries(
            tables[table_name], countries_file=paths.country_mapping_path, warn_on_unused_countries=False
        )

        # Calculate the number of events and the total impact (for each metric) per country-year.
        # Note that the data on the socio-economic impacts is very sparse.
        # For example, the percentage of events that lack data on "deaths" is
        # 66% for earthquakes, 90% for tsunamis, and 50% for volcanoes.
        # len(tb_earthquakes[tb_earthquakes["deaths"].isnull()]) / len(tb_earthquakes) * 100
        # len(tb_tsunamis[tb_tsunamis["deaths"].isnull()]) / len(tb_tsunamis) * 100
        # len(tb_volcanoes[tb_volcanoes["deaths"].isnull()]) / len(tb_volcanoes) * 100
        # Also, note the caveats mentioned in:
        # https://www.ngdc.noaa.gov/hazard/tsunami-db-intro.html#uncertainty
        # Define aggregates.
        aggregates = {column: "sum" for column in tables[table_name].columns if column not in ["country", "year"]}
        # Include another aggregate which simply counts events.
        aggregates.update({"id": "count"})
        # Sum the impact of all metrics.
        # tables[table_name] = (
        #     tables[table_name]
        #     .groupby(["country", "year"], observed=True, as_index=False)
        #     .agg(aggregates)
        #     .rename(columns={"id": "n_events"}, errors="raise")
        #     .assign(**{"type": table_name})
        # )
        tables[table_name] = (
            dataframes.groupby_agg(
                df=tables[table_name], groupby_columns=["country", "year"], aggregations=aggregates, min_num_values=1
            )
            .rename(columns={"id": "n_events"}, errors="raise")
            .reset_index()
            .assign(**{"type": table_name})
        )

    # TODO: Usually "deathstotal" is more often informed than "deaths", but there are also cases where there is
    #  "deaths" but not "deathstotal". Since "deathstotal" includes all deaths (including secondary ones), it would make
    #  sense to fill empty "deathstotal" with "deaths".
    #  However, note that there are events where "deaths" > "deathstotal", which should not happen.
    #  These are probably data issues. Contact the source about it.

    # Merge all tables.
    tb = pr.concat(list(tables.values()), ignore_index=True)

    # For convenience, convert economic damages from millions of dollars to dollars.
    for column in ["damage", "damage_total"]:
        tb[column] *= 1e6

    # Add aggregate regions to table.
    tb = geo.add_regions_to_table(
        tb=tb,
        index_columns=["country", "year", "type"],
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
    )

    # Create a decadal table.
    # tb_decadal = create_decadal_average(tb)
    # NOTE: Alternatively, we could have a table with the decadal total, instead of the average.
    # tb_decadal = create_decadal_total(tb)

    # Format tables conveniently.
    tb_yearly = tb.format(keys=["country", "year", "type"], short_name="natural_hazards")
    # tb_decadal = tb_decadal.format(keys=["country", "year", "type"], short_name="natural_hazards_decadal")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_yearly], check_variables_metadata=True)
    ds_garden.save()
