"""Load a meadow dataset and create a garden dataset.

NOTES:
- In the 2016 and 2017 excel files, the estimated number of farmed fish only count fish with an estimated mean weight (EMW). To understand this, see that, at the top of the "Farmed fishes" sheet they have a table clarifying that the total number of fish is (for 2017) between 42 and 138 billion fish per year. But then there is an additional 17% of fish tonnage that corresponds to fish without an EMW. So, the total global number, after including this missing 17% of tonnage, is between 51 and 167 billion.
- For the data from 2020 onwards, generic mean weights (GEMWs) are used whenever there is no EMW, both for country and for global data. Therefore, there might be a spurious jump between pre and post 2020 data (corresponding to about 17% of the global tonnage). This jump may be more pronounced in countries with a higher proportion of fish without an EMW.
- For the data from 2020 onwards, there are cases of "<1" in the data. We replace those with the average value in that range, namely 0.5 (which means 500,000 fish).
- After combining until-2017 with from-2020 data, there were significant jumps (both up and down) in the data. We decided to keep only the data from 2020 onwards.
- We take the global data from Mood et al. (2023) and combine it with the latest global data from fishcount.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Column names to select and how to rename them, for data until 2017.
COLUMNS_UNTIL_2017 = {
    "country": "country",
    "year": "year",
    "estimated_numbers__millions__lower": "n_farmed_fish_low",
    "estimated_numbers__millions__upper": "n_farmed_fish_high",
    # There is no midpoint column in the data, but it will be calculated.
}

# Columns to select and how to rename them, for data from 2020 onwards.
COLUMNS_FROM_2020 = {
    "country": "country",
    "year": "year",
    "numbers__lower__in_millions": "n_farmed_fish_low",
    "numbers__midpoint__in_millions": "n_farmed_fish",
    "numbers__upper__in_millions": "n_farmed_fish_high",
}

# Columns to select and how to rename them, for historical data from Mood et al. (2023).
COLUMNS_HISTORICAL = {
    "country": "country",
    "year": "year",
    "n_fish_low": "n_farmed_fish_low",
    "n_fish": "n_farmed_fish",
    "n_fish_high": "n_farmed_fish_high",
}

# Regions to create aggregates for.
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
    "World",
]


def sanity_check_inputs_until_2017(tb_until_2017: Table) -> None:
    # Sanity checks for data until 2017.
    for year in [2015, 2016, 2017]:
        # Calculate the lower and upper bounds of the number of fish with or without an EMW.
        lower = tb_until_2017[(tb_until_2017["country"] != "Totals") & (tb_until_2017["year"] == year)][
            "estimated_numbers__millions__lower"
        ].sum()
        upper = tb_until_2017[(tb_until_2017["country"] != "Totals") & (tb_until_2017["year"] == year)][
            "estimated_numbers__millions__upper"
        ].sum()

        # Calculate the lower and upper bounds of the number of fish for which there is an EMW.
        lower_emw = tb_until_2017[
            (tb_until_2017["country"] != "Totals")
            & (tb_until_2017["year"] == year)
            & (tb_until_2017["estimated_mean_weight__lower"] > 0)
        ]["estimated_numbers__millions__lower"].sum()
        upper_emw = tb_until_2017[
            (tb_until_2017["country"] != "Totals")
            & (tb_until_2017["year"] == year)
            & (tb_until_2017["estimated_mean_weight__upper"] > 0)
        ]["estimated_numbers__millions__upper"].sum()

        # Check that the lower and upper bounds of "Totals" is equal to the sum of rows with or without EMW.
        assert round(lower, -1) == round(
            tb_until_2017[(tb_until_2017["country"] == "Totals") & (tb_until_2017["year"] == year)][
                "estimated_numbers__millions__lower"
            ].item(),
            -1,
        )
        assert round(upper, -1) == round(
            tb_until_2017[(tb_until_2017["country"] == "Totals") & (tb_until_2017["year"] == year)][
                "estimated_numbers__millions__upper"
            ].item(),
            -1,
        )

        if year == 2015:
            # The estimated number of farmed fish for 2015 includes species with and without an EMW.
            # Check that the number of fish with EMW is smaller by around 20%.
            assert lower_emw * 1.2 < lower
            assert upper_emw * 1.2 < upper
        elif year in [2016, 2017]:
            # The estimated number of farmed fish for 2016 and 2017 includes only species with an EMW.
            assert round(lower_emw, -1) == round(lower, -1)
            assert round(upper_emw, -1) == round(upper, -1)


def sanity_check_inputs_from_2020(tb_from_2020: Table) -> None:
    # Sanity checks for data from 2020 onwards.
    error = "Expected lower limit < midpoint < upper limit."
    assert (tb_from_2020["n_farmed_fish_low"] <= tb_from_2020["n_farmed_fish"]).all(), error
    assert (tb_from_2020["n_farmed_fish"] <= tb_from_2020["n_farmed_fish_high"]).all(), error
    error = "Expected values to be between 0 and 1e12."
    for column in ["n_farmed_fish_low", "n_farmed_fish", "n_farmed_fish_high"]:
        assert (0 < tb_from_2020[column]).all(), error
        assert (tb_from_2020[column] < 1e12).all(), error


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    tb = geo.add_population_to_table(tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["n_farmed_fish_low_per_capita"] = tb["n_farmed_fish_low"] / tb["population"]
    tb["n_farmed_fish_per_capita"] = tb["n_farmed_fish"] / tb["population"]
    tb["n_farmed_fish_high_per_capita"] = tb["n_farmed_fish_high"] / tb["population"]
    # Drop population column.
    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def sanity_check_outputs(tb: Table) -> None:
    # Check that the total agrees with the sum of aggregates from each continent, for non per capita columns.
    tb = tb[[column for column in tb.columns if "per_capita" not in column]].copy()
    # NOTE: This is only expected for data from 2020 onwards. Data until 2017 at the country level is expected to be missing fish without an EMW.
    for year in tb[tb["year"] >= 2020]["year"].unique():
        world = tb[(tb["country"] == "World") & (tb["year"] == year)].reset_index(drop=True).drop(columns=["country"])
        test = (
            tb[
                (tb["year"] == year)
                & (tb["country"].isin(["Africa", "North America", "South America", "Asia", "Europe", "Oceania"]))
            ]
            .groupby("year", as_index=False)
            .sum(numeric_only=True)
        )
        assert (100 * abs(world - test) / world < 1).all().all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("number_of_farmed_fish")
    tb_until_2017 = ds_meadow.read("number_of_farmed_fish_until_2017")
    tb_from_2020 = ds_meadow.read("number_of_farmed_fish_from_2020")

    # Load Mood et al. (2023) historical data and read its main table.
    ds_historical = paths.load_dataset("farmed_finfishes_used_for_food")
    tb_historical = ds_historical.read("farmed_finfishes_used_for_food")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Run sanity checks on inputs for data until 2017.
    sanity_check_inputs_until_2017(tb_until_2017=tb_until_2017)

    # The number of fish for 2015 includes species with and without an EMW, however 2016 and 2017 includes only fish
    # with an EMW. This means that 2015 includes a 17% of additional production. For consistency, include in 2015 only
    # species with an EMW.
    tb_until_2017 = tb_until_2017[tb_until_2017["estimated_mean_weight__lower"] > 0].reset_index(drop=True)

    # Select and rename columns.
    tb_until_2017 = tb_until_2017[list(COLUMNS_UNTIL_2017)].rename(columns=COLUMNS_UNTIL_2017, errors="raise")

    # Add number of fish for each country and year.
    tb_until_2017 = tb_until_2017.groupby(["country", "year"], as_index=False, observed=True).sum(min_count=1)

    # Adapt units.
    tb_until_2017["n_farmed_fish_low"] *= 1e6
    tb_until_2017["n_farmed_fish_high"] *= 1e6

    # Add midpoint number of farmed fish.
    tb_until_2017["n_farmed_fish"] = (tb_until_2017["n_farmed_fish_low"] + tb_until_2017["n_farmed_fish_high"]) / 2

    # Select and rename columns for data from 2020.
    tb_from_2020 = tb_from_2020[list(COLUMNS_FROM_2020)].rename(columns=COLUMNS_FROM_2020, errors="raise")

    # Prepare data from 2020 onwards.
    # There are cases of "<1" in the data. Replace those with the average value in that range, namely 0.5.
    # Convert to float and change from million fish to fish.
    for column in ["n_farmed_fish_low", "n_farmed_fish", "n_farmed_fish_high"]:
        tb_from_2020[column] = tb_from_2020[column].replace("< 1", "0.5").astype("Float64") * 1e6

    # Run sanity checks on inputs for data from 2020.
    sanity_check_inputs_from_2020(tb_from_2020=tb_from_2020)

    # Combine data until 2017 with data from 2020.
    # NOTE: There is a very big change between pre-2018 and post-2019 data, sometimes it becomes significantly larger, sometimes significantly smaller. In the end we decided to keep only the data from 2020 onwards.
    # tb = pr.concat([tb_until_2017, tb_from_2020], ignore_index=True)
    tb = pr.concat([tb_from_2020], ignore_index=True)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # In data from 2020 onwards, Zanzibar is separate from Tanzania. Merge it with Tanzania (the difference is tiny in any case).
    tb = pr.concat(
        [
            tb[tb["country"] != "Tanzania"],
            tb[tb["country"] == "Tanzania"]
            .groupby("year", as_index=False)
            .agg({column: "sum" for column in ["n_farmed_fish_low", "n_farmed_fish", "n_farmed_fish_high"]})
            .assign(**{"country": "Tanzania"}),
        ],
        ignore_index=True,
    )

    # Add region aggregates.
    # NOTE: Data from 2020 onwards contains global data. We will ignore it to create our own aggregate.
    tb = geo.add_regions_to_table(
        tb=tb[~tb["country"].isin(REGIONS_TO_ADD)],
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
    )

    # Select and rename columns in historical global data from Mood et al. (2023).
    tb_historical = (
        tb_historical[list(COLUMNS_HISTORICAL)]
        .rename(columns=COLUMNS_HISTORICAL, errors="raise")
        .astype({column: "Float64" for column in ["n_farmed_fish_low", "n_farmed_fish", "n_farmed_fish_high"]})
    )

    # Combine with historical (only global) data from Mood et al. (2023). Where there is overlap (prior to 2019) prioritize historical data (which contains data for fish without an EMW).
    tb = combine_two_overlapping_dataframes(df1=tb_historical, df2=tb, index_columns=["country", "year"])

    # Add per capita number of farmed fish.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb)

    # Improve table format.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Instead of having an attribution for each individual snapshot, add a single attribution, with the latest year of all origins.
    publication_year = max([origin.date_published for origin in tb["n_farmed_fish"].m.origins])

    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, yaml_params={"publication_year": publication_year}
    )
    ds_garden.save()
