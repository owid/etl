import numpy as np
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Column names for the years are different by sex
BOTH_SEXES = ["_1990", "_1995", "_2000", "_2005", "_2010", "_2015", "_2020", "_2024"]
MALES = ["_1990_1", "_1995_1", "_2000_1", "_2005_1", "_2010_1", "_2015_1", "_2020_1", "_2024_1"]
FEMALES = ["_1990_2", "_1995_2", "_2000_2", "_2005_2", "_2010_2", "_2015_2", "_2020_2", "_2024_2"]

ALL_YEARS = BOTH_SEXES + MALES + FEMALES

REGIONS = list(geo.REGIONS.keys()) + ["World"]

EXCL_REGIONS = [
    "Developed regions",
    "Australia and New Zealand",
    "Caribbean",
    "Less developed regions, excluding least developed countries",
    "Less developed regions, excluding China",
    "Least developed countries",
    "Less developed regions",
    "More developed regions",
    "Australia/New Zealand",
    "AFRICA",
    "ASIA",
    "EUROPE",
    "Africa",
    "Asia",
    "Europe",
    "Oceania",
    "OCEANIA",
    "South America",
    "WORLD",
    "World",
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-and-upper-middle-income countries",
    "Low-and-lower-middle-income-countries",
    "Low-and-middle-income-countries",
    "Oceania (excluding Australia and New Zealand)",
    "Middle-income countries",
]


def add_others_to_world(tb, country_col, others_name="Others", rel_columns=None):
    """Add "Others" region to "World" totals, as it isn't included automatically when summing over countries."""
    # add "World" as a region by summing over all countries
    tb_w_o = tb[tb[country_col].isin(["World", others_name])].copy()

    if rel_columns is None:
        rel_columns = [
            "immigrants_all",
            "immigrants_female",
            "immigrants_male",
            "emigrants_all",
            "emigrants_female",
            "emigrants_male",
            "total_population",
            "female_population",
            "male_population",
        ]

    tb_w_o[rel_columns] = tb_w_o[rel_columns].fillna(0)

    tb_world = tb_w_o.groupby("year").sum().reset_index()

    tb_world[country_col] = "World"

    # remove "World" from original table and add the new "World" totals
    tb = tb[~tb[country_col].isin(["World"])]
    tb = pr.concat([tb, tb_world], ignore_index=True)

    return tb


def run() -> None:
    #
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("migrant_stock")
    # Read tables from meadow dataset.
    # destination and origin table
    tb_do = ds_meadow.read("migrant_stock_dest_origin")
    # destination table, total numbers and shares
    tb_d_total = ds_meadow.read("migrant_stock_dest_total")
    tb_d_share = ds_meadow.read("migrant_stock_dest_share")
    tb_d_pop = ds_meadow.read("un_desa_total_population")
    # origin table
    tb_o = ds_meadow.read("migrant_stock_origin")

    # Remove excluded regions from the dataset.
    tb_do = tb_do[~tb_do["country_destination"].isin(EXCL_REGIONS)]
    tb_do = tb_do[~tb_do["country_origin"].isin(EXCL_REGIONS)]

    tb_do = format_table(tb_do, ["country_destination", "country_origin"], "migrants")

    tb_do = tb_do.format(["country_destination", "country_origin", "year"], short_name="migrant_stock_dest_origin")

    ## format data on destination
    tb_d_total = format_table(tb_d_total, ["country"], "migrants")
    tb_d_share = format_table(tb_d_share, ["country"], "migrant_share")

    ## format data on origin
    tb_o = format_table(tb_o, ["country"], "migrants")

    # format total population data
    tb_d_pop = format_table(tb_d_pop, ["country"], "total_population")

    ## all combine tables except for destination and origin table
    tb = combine_all_tables(tb_d_total, tb_d_share, tb_o, tb_d_pop)

    # calculate regions
    agg = {
        "immigrants_all": "sum",
        "immigrants_female": "sum",
        "immigrants_male": "sum",
        "emigrants_all": "sum",
        "emigrants_female": "sum",
        "emigrants_male": "sum",
        "total_population": "sum",
        "female_population": "sum",
        "male_population": "sum",
    }

    tb = paths.regions.add_aggregates(
        tb,
        regions=REGIONS,
        frac_allowed_nans_per_year=0.1,
        country_col="country",
        aggregations=agg,
    )

    tb = add_others_to_world(tb, "country")
    # add channel islands to world totals (so they match world totals in stock data)
    tb = add_others_to_world(tb, "country", others_name="Channel Islands")

    # set column names for readable code
    im_s_all = "immigrant_share_of_dest_population_all"
    im_s_fem = "immigrant_share_of_dest_population_female"
    im_s_male = "immigrant_share_of_dest_population_male"
    em_s_all = "emigrants_share_of_total_population"

    # calculate shares for regions & add metadata
    tb[im_s_all] = np.where(
        tb["country"].isin(REGIONS), (tb["immigrants_all"] / tb["total_population"]) * 100, tb[im_s_all]
    )
    tb[im_s_all] = tb[im_s_all].copy_metadata(tb["immigrants_all"])
    tb[im_s_fem] = np.where(
        tb["country"].isin(REGIONS), (tb["immigrants_female"] / tb["female_population"] * 100), tb[im_s_fem]
    )
    tb[im_s_fem] = tb[im_s_fem].copy_metadata(tb["immigrants_all"])
    tb[im_s_male] = np.where(
        tb["country"].isin(REGIONS), (tb["immigrants_male"] / tb["male_population"] * 100), tb[im_s_male]
    )
    tb[im_s_male] = tb[im_s_male].copy_metadata(tb["immigrants_all"])
    tb[em_s_all] = tb["emigrants_all"] / (tb["total_population"]) * 100

    # drop total population columns
    tb = tb.drop(columns=["total_population", "male_population", "female_population"])

    tb = tb.format(["country", "year"], short_name="migrant_stock")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_do], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def combine_all_tables(tb_d_total, tb_d_share, tb_o, tb_d_pop):
    """Combine all tables except for destination and origin table to one table by renaming column names and merging"""
    tb_d_total = tb_d_total.rename(
        columns={
            "migrants_all_sexes": "immigrants_all",
            "migrants_female": "immigrants_female",
            "migrants_male": "immigrants_male",
        },
        errors="raise",
    )

    tb_d_share = tb_d_share.rename(
        columns={
            "migrant_share_all_sexes": "immigrant_share_of_dest_population_all",
            "migrant_share_female": "immigrant_share_of_dest_population_female",
            "migrant_share_male": "immigrant_share_of_dest_population_male",
        },
        errors="raise",
    )

    tb_o = tb_o.rename(
        columns={
            "migrants_all_sexes": "emigrants_all",
            "migrants_female": "emigrants_female",
            "migrants_male": "emigrants_male",
        },
        errors="raise",
    )

    tb_d_pop = tb_d_pop.rename(
        columns={
            "total_population_all_sexes": "total_population",
            "total_population_female": "female_population",
            "total_population_male": "male_population",
        },
        errors="raise",
    )

    tb = pr.multi_merge([tb_d_total, tb_d_share, tb_o, tb_d_pop], on=["country", "year"], how="outer")

    return tb


def format_table(tb, country_cols, value_name):
    """Formats tables from UN DESA data to have consistent country, year, and value columns by:
    - Melting the table to have a year column
    - Cleaning year & country column to have consistent values
    - Pivoting table to have one row per country and year and values for sex in extra columns
    - Scaling values by thousands where necessary
    - Adding metadata to the table
    - Harmonizing column and country names
    """
    # melt table to remove all extra year columns for different sexes
    tb = tb.melt(
        id_vars=country_cols,
        value_vars=ALL_YEARS,
        var_name="year",
        value_name=value_name,
    )

    # map year to sex
    tb["sex"] = tb["year"].apply(lambda x: year_to_sex(x))

    # clean year column
    tb["year"] = tb["year"].str.split("_").apply(lambda x: int(x[1]))

    # clean country columns
    for col in country_cols:
        tb[col] = tb[col].str.strip()

    # change dtype to numeric
    tb[value_name] = tb[value_name].astype("Float64")

    # pivot table to have one row per country and year and have values for sexes in extra columns
    tb = tb.pivot(index=country_cols + ["year"], columns="sex", values=value_name).reset_index()

    # rename columns
    tb = tb.rename(
        columns={
            "all sexes": f"{value_name}_all_sexes",
            "females": f"{value_name}_female",
            "males": f"{value_name}_male",
        },
        errors="raise",
    )
    # harmonize country names
    for country_col in country_cols:
        tb = paths.regions.harmonize_names(tb, country_col=country_col)

    # remove duplicate data
    tb = tb.drop_duplicates()

    return tb


# Deduce sex for year column name
def year_to_sex(year):
    if year in BOTH_SEXES:
        return "all sexes"
    elif year in MALES:
        return "males"
    elif year in FEMALES:
        return "females"
