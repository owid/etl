"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the table of yearly global numbers (given in billions), and how to rename them.
COLUMNS_GLOBAL = {
    "year": "year",
    "numbers__lower__in_billions": "n_wild_fish_low",
    "numbers__midpoint__in_billions": "n_wild_fish",
    "numbers__upper__in_billions": "n_wild_fish_high",
}

# Columns to select from the table of numbers by country (given in millions), and how to rename them.
COLUMNS_BY_COUNTRY = {
    "country": "country",
    "numbers__lower__in_millions": "n_wild_fish_low",
    "numbers__midpoint__in_millions": "n_wild_fish",
    "numbers__upper__in_millions": "n_wild_fish_high",
}

# Columns to select from the table of yearly global numbers in Mood and Brooke (2024) (given in millions), and how to
# rename them.
COLUMNS_PAPER = {
    "year": "year",
    "estimated_numbers_in_millions__lower": "n_wild_fish_low",
    "estimated_numbers_in_millions__midpoint": "n_wild_fish",
    "estimated_numbers_in_millions__upper": "n_wild_fish_high",
}

# Columns to select from the table of fish caught for fishmeal and fish oil (given in billions), and how to rename them.
COLUMNS_FISHMEAL = {
    "year": "year",
    "finfish_capture_for_reduction__estimated_numbers__billions__lower": "n_wild_fish_for_fishmeal_and_fish_oil_low",
    "finfish_capture_for_reduction__estimated_numbers__billions__upper": "n_wild_fish_for_fishmeal_and_fish_oil_high",
}

# Labels of the rows of averages and totals in the original tables.
LABEL_GLOBAL_AVERAGE = "Average"
LABEL_COUNTRY_TOTAL = "Totals for all countries"
LABEL_FISHMEAL_AVERAGE = "Average 2015-24"

# Regions for which aggregates will be created.
REGIONS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "European Union (27)",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("number_of_wild_fish_killed_for_food")
    tb_global = ds_meadow.read("number_of_wild_fish_killed_for_food_global")
    tb_by_country = ds_meadow.read("number_of_wild_fish_killed_for_food_by_country")
    tb_fishmeal = ds_meadow.read("number_of_wild_fish_caught_for_fishmeal_and_fish_oil")
    tb_paper = ds_meadow.read("number_of_wild_fish_killed_for_food_global_from_paper")

    #
    # Process data.
    #
    # Select and rename columns.
    tb_global = tb_global[list(COLUMNS_GLOBAL)].rename(columns=COLUMNS_GLOBAL, errors="raise")
    tb_by_country = tb_by_country[list(COLUMNS_BY_COUNTRY)].rename(columns=COLUMNS_BY_COUNTRY, errors="raise")
    tb_fishmeal = tb_fishmeal[list(COLUMNS_FISHMEAL)].rename(columns=COLUMNS_FISHMEAL, errors="raise")
    tb_paper = tb_paper[list(COLUMNS_PAPER)].rename(columns=COLUMNS_PAPER, errors="raise")

    # Small countries have values given as "< 1" (less than one million fish); treat them as missing.
    for column in ["n_wild_fish_low", "n_wild_fish", "n_wild_fish_high"]:
        tb_by_country.loc[tb_by_country[column] == "< 1", column] = None
        tb_by_country[column] = pr.to_numeric(tb_by_country[column])

    # Run sanity checks on inputs.
    sanity_check_inputs(tb_global=tb_global, tb_by_country=tb_by_country, tb_fishmeal=tb_fishmeal, tb_paper=tb_paper)

    # Remove rows of averages and totals (used only in sanity checks).
    tb_global = tb_global[tb_global["year"] != LABEL_GLOBAL_AVERAGE].astype({"year": int}).reset_index(drop=True)
    tb_by_country = tb_by_country[tb_by_country["country"] != LABEL_COUNTRY_TOTAL].reset_index(drop=True)
    tb_fishmeal = (
        tb_fishmeal[tb_fishmeal["year"] != LABEL_FISHMEAL_AVERAGE].astype({"year": int}).reset_index(drop=True)
    )

    # Convert units from billions and millions to numbers of fish.
    for column in ["n_wild_fish_low", "n_wild_fish", "n_wild_fish_high"]:
        tb_global[column] = tb_global[column] * 1e9
        tb_by_country[column] = tb_by_country[column] * 1e6
        tb_paper[column] = tb_paper[column] * 1e6
    for column in ["n_wild_fish_for_fishmeal_and_fish_oil_low", "n_wild_fish_for_fishmeal_and_fish_oil_high"]:
        tb_fishmeal[column] = tb_fishmeal[column] * 1e9

    # Country data are averages over the whole period of the yearly data; assign them to the last year of that period.
    first_year_of_country_average = tb_global["year"].min()
    tb_by_country["year"] = tb_global["year"].max()

    # Fishcount's website starts in 2003; add the earlier years from Mood and Brooke (2024), which uses the same method.
    tb_global = pr.concat([tb_paper[tb_paper["year"] < first_year_of_country_average], tb_global], ignore_index=True)
    tb_global["country"] = "World"

    # Combine global and country data.
    tb = pr.concat([tb_global, tb_by_country], ignore_index=True)

    # Harmonize country names.
    # NOTE: Since country data is assigned to the last year, we exclude historical regions (like Netherlands Antilles).
    tb = paths.regions.harmonize_names(tb=tb)

    # Fishcount lists Zanzibar apart from the rest of Tanzania (as the FAO does); add them up into Tanzania.
    duplicated = set(tb[tb.duplicated(subset=["country", "year"])]["country"])
    assert duplicated == {"Tanzania"}, f"Unexpected countries with more than one row: {duplicated}"
    tb = tb.groupby(["country", "year"], as_index=False, observed=True).sum(min_count=1)

    # Add region aggregates.
    tb = paths.regions.add_aggregates(tb=tb, regions=REGIONS, min_num_values_per_year=1)

    # Add per capita numbers.
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    for column in ["n_wild_fish_low", "n_wild_fish", "n_wild_fish_high"]:
        tb[f"{column}_per_capita"] = tb[column] / tb["population"]
    tb = tb.drop(columns=["population"], errors="raise")

    # Prepare table of fish caught for fishmeal and fish oil.
    # The source gives no midpoint; as in Fishcount's other estimates, take the average of the lower and upper bounds.
    tb_fishmeal["n_wild_fish_for_fishmeal_and_fish_oil"] = (
        tb_fishmeal["n_wild_fish_for_fishmeal_and_fish_oil_low"]
        + tb_fishmeal["n_wild_fish_for_fishmeal_and_fish_oil_high"]
    ) / 2
    tb_fishmeal["country"] = "World"

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb, tb_fishmeal=tb_fishmeal, first_year_of_country_average=first_year_of_country_average)

    # Improve table formats.
    tb = tb.format(short_name=paths.short_name)
    tb_fishmeal = tb_fishmeal.format(short_name="number_of_wild_fish_caught_for_fishmeal_and_fish_oil")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_fishmeal])
    ds_garden.save()


def sanity_check_inputs(tb_global: Table, tb_by_country: Table, tb_fishmeal: Table, tb_paper: Table) -> None:
    columns = ["n_wild_fish_low", "n_wild_fish", "n_wild_fish_high"]
    global_average = tb_global[tb_global["year"] == LABEL_GLOBAL_AVERAGE][columns]
    country_total = tb_by_country[tb_by_country["country"] == LABEL_COUNTRY_TOTAL][columns]
    assert len(global_average) == 1, "Expected one row of global averages."
    assert len(country_total) == 1, "Expected one row of totals for all countries."
    years = tb_global[tb_global["year"] != LABEL_GLOBAL_AVERAGE]["year"].astype(int)
    assert list(years) == list(range(years.min(), years.max() + 1)), "Expected consecutive years in the global data."

    # The total for all countries (in millions) should equal the global average (in billions).
    assert (country_total.values[0] == 1000 * global_average.values[0]).all(), (
        "The total for all countries differs from the global average."
    )

    # The sum of all countries should be close to the total (they differ because the source rounds values to two
    # significant figures).
    countries = tb_by_country[tb_by_country["country"] != LABEL_COUNTRY_TOTAL]
    error = 100 * abs(countries[columns].sum() - country_total.values[0]) / country_total.values[0]
    assert (error < 5).all(), f"The sum of all countries differs from the total by more than 5%: {error.to_dict()}"

    # The global average should be close to the average of all years (they differ because the source rounds values).
    yearly = tb_global[tb_global["year"] != LABEL_GLOBAL_AVERAGE][columns].astype(float)
    error = 100 * abs(yearly.mean() - global_average.values[0]) / global_average.values[0]
    assert (error < 5).all(), (
        f"The global average differs from the mean of all years by more than 5%: {error.to_dict()}"
    )

    # The world totals on Fishcount's website (in billions) and in Mood and Brooke (2024) (in millions) should agree in
    # the years they share (they differ slightly because both round values to two significant figures).
    tb_compare = yearly.assign(year=years).merge(tb_paper, on="year", how="inner", suffixes=("_website", "_paper"))
    assert len(tb_compare) > 10, "Expected Fishcount's website and Mood and Brooke (2024) to share many years."
    for column in columns:
        error = (
            100
            * abs(1000 * tb_compare[f"{column}_website"] - tb_compare[f"{column}_paper"])
            / tb_compare[f"{column}_paper"]
        )
        assert (error < 5).all(), (
            f"Fishcount's website and Mood and Brooke (2024) differ by more than 5% in {column}: {error.max():.1f}%"
        )

    # The fishmeal average (over the last 10 years) should be close to the mean of those years.
    columns = ["n_wild_fish_for_fishmeal_and_fish_oil_low", "n_wild_fish_for_fishmeal_and_fish_oil_high"]
    fishmeal_average = tb_fishmeal[tb_fishmeal["year"] == LABEL_FISHMEAL_AVERAGE][columns]
    assert len(fishmeal_average) == 1, "Expected one row of averages in the fishmeal data."
    fishmeal_yearly = tb_fishmeal[tb_fishmeal["year"] != LABEL_FISHMEAL_AVERAGE].astype({"year": int})
    fishmeal_yearly = fishmeal_yearly[fishmeal_yearly["year"] > fishmeal_yearly["year"].max() - 10][columns]
    error = 100 * abs(fishmeal_yearly.astype(float).mean() - fishmeal_average.values[0]) / fishmeal_average.values[0]
    assert (error < 5).all(), f"The fishmeal average differs from the mean of the last 10 years: {error.to_dict()}"


def sanity_check_outputs(tb: Table, tb_fishmeal: Table, first_year_of_country_average: int) -> None:
    for table, column in [(tb, "n_wild_fish"), (tb_fishmeal, "n_wild_fish_for_fishmeal_and_fish_oil")]:
        error = "Expected lower bound <= midpoint <= upper bound."
        assert (table[f"{column}_low"].dropna() >= 0).all(), "Unexpected negative values."
        _tb = table.dropna(subset=[f"{column}_low", column])
        assert (_tb[f"{column}_low"] <= _tb[column]).all(), error
        _tb = table.dropna(subset=[column, f"{column}_high"])
        assert (_tb[column] <= _tb[f"{column}_high"]).all(), error

    # The world total should be between 1 and 3 trillion fish (the source estimates 1.1-2.2 trillion fish on average).
    world = tb[tb["country"] == "World"]
    assert list(world["year"]) == list(range(1999, world["year"].max() + 1)), "Expected world totals for every year."
    assert (world["n_wild_fish_low"] > 5e11).all() and (world["n_wild_fish_high"] < 3e12).all(), (
        "Unexpected magnitude of world totals; check unit conversions."
    )

    # The sum of continents should be close to the global average (since country data is an average over all years).
    world_average = world[world["year"] >= first_year_of_country_average][
        ["n_wild_fish_low", "n_wild_fish", "n_wild_fish_high"]
    ].mean()
    continents = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]
    continents_sum = tb[tb["country"].isin(continents)][["n_wild_fish_low", "n_wild_fish", "n_wild_fish_high"]].sum()
    error = 100 * abs(continents_sum - world_average) / world_average
    assert (error < 5).all(), f"The sum of continents differs from the global average: {error.to_dict()}"

    # Fish caught for fishmeal and fish oil are a part of all wild-caught fish.
    tb_compare = tb_fishmeal.merge(world, on=["country", "year"], how="inner")
    assert (tb_compare["n_wild_fish_for_fishmeal_and_fish_oil"] < tb_compare["n_wild_fish"]).all(), (
        "Fish caught for fishmeal and fish oil should be fewer than all wild-caught fish."
    )
