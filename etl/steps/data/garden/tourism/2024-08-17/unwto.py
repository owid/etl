"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.999
# Define accepted overlaps
ACCEPTED_OVERLAPS = [
    {year: {"Serbia and Montenegro", "Serbia"} for year in range(1995, 2023)},
    {year: {"Montenegro", "Serbia and Montenegro"} for year in range(1995, 2023)},
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unwto")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["unwto"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Drop columns with all NaN values
    tb = tb.dropna(axis=1, how="all")

    # Find columns that start with the specified prefixes - units are not thousands
    prefixes_not_thousands = ["inbound_tourism_expenditure", "outbound_tourism_expenditure", "tourism_industries"]
    matching_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in prefixes_not_thousands)]

    # Multiply the all the other columns by 1000 as these are in thousands
    for col in tb.columns:
        if col not in matching_columns + ["country", "year"]:
            tb[col] = tb[col] * 1000

    # Shortern column names
    tb.columns = shorten_column_names(tb.columns)

    # Find expenditure columns that are in US million and convert
    prefixes_us_dollars = ["inbound_tourism_expenditure", "outbound_tourism_expenditure"]
    matching_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in prefixes_us_dollars)]

    for col in matching_columns:
        tb[col] = tb[col] * 1e6

    # Calculate the business/personal ratio column
    tb["business_personal_ratio"] = tb["in_tour_purpose_business_and_prof"] / tb["in_tour_purpose_personal"]
    # Calculate the inbound/outbound ratio ratio column
    tb["inbound_outbound_tourism"] = (
        tb["in_tour_arrivals_ovn_vis_tourists"] / tb["out_tour_departures_ovn_vis_tourists"]
    )
    # Calculate same-day by tourist trips ratio
    tb["same_day_tourist_ratio"] = tb["in_tour_arrivals_same_day_vis_excur"] / tb["in_tour_arrivals_ovn_vis_tourists"]
    # Add certain indicators per 1,000 inhabitants
    tb = geo.add_population_to_table(tb, ds_population)
    #
    # Calculate per 1,000 inhabitants for some indicators
    #
    cols_per_1000 = [
        "in_tour_arrivals_ovn_vis_tourists",
        "in_tour_arrivals_same_day_vis_excur",
        "out_tour_departures_ovn_vis_tourists",
        "dom_tour_trips_ovn_vis_tourists",
        "dom_tour_trips_same_day_vis_excur",
        "employment_food_and_beverage_serving_act",
        "employment_total",
    ]
    for col in cols_per_1000:
        tb[f"{col}_per_1000"] = tb[col] / (tb["population"] / 1000)
    tb = tb.drop(columns=["population"])
    #
    # Calculate the inbound tourism by region
    #
    region_columns = [
        "in_tour_regions_africa",
        "in_tour_regions_americas",
        "in_tour_regions_east_asia_and_the_pacific",
        "in_tour_regions_europe",
        "in_tour_regions_middle_east",
        "in_tour_regions_other_not_class",
        "in_tour_regions_south_asia",
    ]

    tb_regions_sum = tb[region_columns + ["year"]].groupby("year").sum().reset_index()

    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        regions=REGIONS,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
        accepted_overlaps=ACCEPTED_OVERLAPS,
    )
    # Rename the columns
    rename_mapping = {
        "in_tour_regions_africa": "Africa (UNWTO)",
        "in_tour_regions_americas": "Americas (UNWTO)",
        "in_tour_regions_east_asia_and_the_pacific": "East Asia and the Pacific (UNWTO)",
        "in_tour_regions_europe": "Europe (UNWTO)",
        "in_tour_regions_middle_east": "Middle East (UNWTO)",
        "in_tour_regions_other_not_class": "Other Not Classified (UNWTO)",
        "in_tour_regions_south_asia": "South Asia (UNWTO)",
    }
    tb_regions_sum = tb_regions_sum.rename(columns=rename_mapping)

    # Melt the Table so that each region is a row in a country column
    tb_regions_sum = pd.melt(
        tb_regions_sum, id_vars=["year"], var_name="country", value_name="inbound_tourism_by_region"
    )
    tb = pr.concat([tb, tb_regions_sum], axis=0)

    # Add origins to the new column
    tb["inbound_tourism_by_region"].metadata.origins = tb["in_tour_regions_africa"].metadata.origins

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


def shorten_column_names(columns):
    # Define common abbreviations and replacements
    abbr = {
        "inbound": "in",
        "outbound": "out",
        "tourism": "tour",
        "domestic": "dom",
        "accommodation": "accom",
        "establishments": "estab",
        "visitors": "vis",
        "passengers": "pass",
        "excursionists": "excur",
        "overnights": "ovn",
        "expenditure": "exp",
        "industries": "ind",
        "transportation": "trans",
        "activities": "act",
        "professional": "prof",
        "nationals": "nat",
        "residing": "res",
        "classified": "class",
        "available": "avail",
        "capacity": "cap",
        "average": "avg",
        "number": "num",
        "occupancy": "occ",
    }

    short_columns = []

    for col in columns:
        # Split the column name into parts
        parts = col.split("__")

        # Shorten each part
        short_parts = []
        for part in parts:
            words = part.split("_")
            short_words = [abbr.get(word, word[:10]) for word in words]
            short_parts.append("_".join(short_words))

        # Join the shortened parts
        short_col = "_".join(short_parts)

        # Remove duplicate words
        short_col = "_".join(dict.fromkeys(short_col.split("_")))

        short_columns.append(short_col)

    return short_columns
