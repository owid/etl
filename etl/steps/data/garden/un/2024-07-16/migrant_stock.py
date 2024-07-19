import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


REGIONS = [
    "WORLD",
    "Sub-Saharan Africa",
    "Northern Africa and Western Asia",
    "Central and Southern Asia",
    "Eastern and South-Eastern Asia",
    "Latin America and the Caribbean",
    "Oceania (excluding Australia and New Zealand)",
    "Australia and New Zealand",
    "Europe and Northern America",
    "Developed regions",
    "Less developed regions",
    "Less developed regions, excluding least developed countries",
    "Less developed regions, excluding China",
    "Least developed countries",
    "Land-locked Developing Countries (LLDC)",
    "Small island developing States (SIDS)",
    "High-income countries",
    "Middle-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
    "AFRICA",
    "Eastern Africa",
    "Middle Africa",
    "Northern Africa",
    "Southern Africa",
    "Western Africa",
    "ASIA",
    "Central Asia",
    "Eastern Asia",
    "South-Eastern Asia",
    "Southern Asia",
    "Western Asia",
    "EUROPE",
    "Eastern Europe",
    "Northern Europe",
    "Southern Europe",
    "Western Europe",
    "LATIN AMERICA AND THE CARIBBEAN",
    "Caribbean",
    "Central America",
    "South America",
    "NORTHERN AMERICA",
    "OCEANIA",
    "Australia and New Zealand",
    "Melanesia",
    "Micronesia",
    "Polynesia*",
]

BOTH_SEXES = ["_1990", "_1995", "_2000", "_2005", "_2010", "_2015", "_2020"]
MALES = ["_1990_1", "_1995_1", "_2000_1", "_2005_1", "_2010_1", "_2015_1", "_2020_1"]
FEMALES = ["_1990_2", "_1995_2", "_2000_2", "_2005_2", "_2010_2", "_2015_2", "_2020_2"]

ALL_YEARS = BOTH_SEXES + MALES + FEMALES

SA_COLS_RENAME = {
    "_0_4": "all_immigrants_aged_0_to_4",
    "_5_9": "all_immigrants_aged_5_to_9",
    "_10_14": "all_immigrants_aged_10_to_14",
    "_15_19": "all_immigrants_aged_15_to_19",
    "_20_24": "all_immigrants_aged_20_to_24",
    "_25_29": "all_immigrants_aged_25_to_29",
    "_30_34": "all_immigrants_aged_30_to_34",
    "_35_39": "all_immigrants_aged_35_to_39",
    "_40_44": "all_immigrants_aged_40_to_44",
    "_45_49": "all_immigrants_aged_45_to_49",
    "_50_54": "all_immigrants_aged_50_to_54",
    "_55_59": "all_immigrants_aged_55_to_59",
    "_60_64": "all_immigrants_aged_60_to_64",
    "_65_69": "all_immigrants_aged_65_to_69",
    "_70_74": "all_immigrants_aged_70_to_74",
    "_75plus": "all_immigrants_aged_75_plus",
    "_0_4_1": "male_immigrants_aged_0_to_4",
    "_5_9_1": "male_immigrants_aged_5_to_9",
    "_10_14_1": "male_immigrants_aged_10_to_14",
    "_15_19_1": "male_immigrants_aged_15_to_19",
    "_20_24_1": "male_immigrants_aged_20_to_24",
    "_25_29_1": "male_immigrants_aged_25_to_29",
    "_30_34_1": "male_immigrants_aged_30_to_34",
    "_35_39_1": "male_immigrants_aged_35_to_39",
    "_40_44_1": "male_immigrants_aged_40_to_44",
    "_45_49_1": "male_immigrants_aged_45_to_49",
    "_50_54_1": "male_immigrants_aged_50_to_54",
    "_55_59_1": "male_immigrants_aged_55_to_59",
    "_60_64_1": "male_immigrants_aged_60_to_64",
    "_65_69_1": "male_immigrants_aged_65_to_69",
    "_70_74_1": "male_immigrants_aged_70_to_74",
    "_75plus_1": "male_immigrants_aged_75_plus",
    "_0_4_2": "female_immigrants_aged_0_to_4",
    "_5_9_2": "female_immigrants_aged_5_to_9",
    "_10_14_2": "female_immigrants_aged_10_to_14",
    "_15_19_2": "female_immigrants_aged_15_to_19",
    "_20_24_2": "female_immigrants_aged_20_to_24",
    "_25_29_2": "female_immigrants_aged_25_to_29",
    "_30_34_2": "female_immigrants_aged_30_to_34",
    "_35_39_2": "female_immigrants_aged_35_to_39",
    "_40_44_2": "female_immigrants_aged_40_to_44",
    "_45_49_2": "female_immigrants_aged_45_to_49",
    "_50_54_2": "female_immigrants_aged_50_to_54",
    "_55_59_2": "female_immigrants_aged_55_to_59",
    "_60_64_2": "female_immigrants_aged_60_to_64",
    "_65_69_2": "female_immigrants_aged_65_to_69",
    "_70_74_2": "female_immigrants_aged_70_to_74",
    "_75plus_2": "female_immigrants_aged_75_plus",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("migrant_stock")

    # Read tables from meadow dataset.
    tb_do = ds_meadow["migrant_stock_dest_origin"].reset_index()
    tb_d_total = ds_meadow["migrant_stock_dest_total"].reset_index()
    tb_d_share = ds_meadow["migrant_stock_dest_share"].reset_index()
    tb_o = ds_meadow["migrant_stock_origin"].reset_index()
    tb_sa_total = ds_meadow["migrant_stock_sex_age_total"].reset_index()
    tb_sa_share = ds_meadow["migrant_stock_sex_age_share"].reset_index()
    tb_pop = ds_meadow["total_population"].reset_index()

    ## data on destination and origin
    # Remove aggregated regions from the dataset.
    tb_do = tb_do[~tb_do["country_destination"].isin(REGIONS)]
    tb_do = tb_do[~tb_do["country_origin"].isin(REGIONS)]

    tb_do = format_table(tb_do, ["country_destination", "country_origin"], "migrants", value_factor=1000)

    tb_do = tb_do.format(["country_destination", "country_origin", "year"])

    ## data on destination
    tb_d_total = format_table(tb_d_total, ["country"], "migrants", value_factor=1000)
    tb_d_share = format_table(tb_d_share, ["country"], "migrant_share")

    ## data on origin
    tb_o = format_table(tb_o, ["country"], "migrants")

    ## data on sex and age
    sa_share_cols_rename = {key: "share_of_" + value for key, value in SA_COLS_RENAME.items()}

    tb_sa_total = tb_sa_total.rename(columns=SA_COLS_RENAME)
    tb_sa_share = tb_sa_share.rename(columns=sa_share_cols_rename)

    for col in SA_COLS_RENAME.values():
        tb_sa_total[col] = pd.to_numeric(tb_sa_total[col], errors="coerce")
        tb_sa_total[col] = tb_sa_total[col] * 1000

    tb_sa_total = add_metadata(tb_sa_total, list(SA_COLS_RENAME.values()), "year")

    tb_sa_total = tb_sa_total.drop(columns=["total", "total_1", "total_2"])
    tb_sa_share = tb_sa_share.drop(columns=["total", "total_1", "total_2"])

    tb_sa_total = geo.harmonize_countries(
        df=tb_sa_total, countries_file=paths.country_mapping_path, country_col="country", warn_on_unused_countries=False
    )
    tb_sa_share = geo.harmonize_countries(
        df=tb_sa_share, countries_file=paths.country_mapping_path, country_col="country", warn_on_unused_countries=False
    )

    tb_sa_total = tb_sa_total.drop_duplicates()
    tb_sa_share = tb_sa_share.drop_duplicates()

    ## population data
    for col in tb_pop.columns[2:]:
        tb_pop[col] = pd.to_numeric(tb_pop[col], errors="coerce")
        tb_pop[col] = tb_pop[col] * 1000
    tb_pop = geo.harmonize_countries(
        df=tb_pop, countries_file=paths.country_mapping_path, country_col="country", warn_on_unused_countries=False
    )
    tb_pop = tb_pop.drop_duplicates()

    # combine tables except for destination and origin table
    tb_d_total = tb_d_total.rename(
        columns={
            "migrants_all_sexes": "immigrants_all",
            "migrants_female": "immigrants_female",
            "migrants_male": "immigrants_male",
        }
    )

    tb_d_share = tb_d_share.rename(
        columns={
            "migrant_share_all_sexes": "immigrant_share_of_dest_population_all",
            "migrant_share_female": "immigrant_share_of_dest_population_female",
            "migrant_share_male": "immigrant_share_of_dest_population_male",
        }
    )

    tb_o = tb_o.rename(
        columns={
            "migrants_all_sexes": "emigrants_all",
            "migrants_female": "emigrants_female",
            "migrants_male": "emigrants_male",
        }
    )

    tb = pr.multi_merge(
        [tb_d_total, tb_d_share, tb_o, tb_sa_total, tb_sa_share, tb_pop], on=["country", "year"], how="outer"
    )
    tb.metadata.short_name = "migrant_stock"

    # Calculate missing values: under 15 y/o migrants, under 20 y/o migrants, share of these in total population, 5 year change in migrants, share of total population
    tb["immigrants_under_15"] = (
        tb["all_immigrants_aged_0_to_4"] + tb["all_immigrants_aged_5_to_9"] + tb["all_immigrants_aged_10_to_14"]
    )
    tb["immigrants_under_20"] = tb["immigrants_under_15"] + tb["all_immigrants_aged_15_to_19"]

    tb["share_of_immigrants_under_15"] = tb["immigrants_under_15"] / (tb["total_population"] / 1000)
    tb["share_of_immigrants_under_20"] = tb["immigrants_under_20"] / (tb["total_population"] / 1000)

    tb["immigrants_change_5_years"] = tb.apply(lambda x: migrant_change_5_years(tb, x, "immigrants_all"), axis=1)
    tb["emigrants_change_5_years"] = tb.apply(lambda x: migrant_change_5_years(tb, x, "emigrants_all"), axis=1)

    tb["immigrants_change_5_years_per_1000"] = tb["immigrants_change_5_years"] / (tb["total_population"] / 1000)
    tb["emigrants_change_5_years_per_1000"] = tb["emigrants_change_5_years"] / (tb["total_population"] / 1000)

    tb["emigrants_share_of_total_population"] = tb["emigrants_all"] / (tb["total_population"]) * 100

    change_cols = [
        "immigrants_change_5_years",
        "emigrants_change_5_years",
        "immigrants_change_5_years_per_1000",
        "emigrants_change_5_years_per_1000",
    ]

    for col in change_cols:
        tb[col] = pd.to_numeric(tb[col], errors="coerce")

    tb = add_metadata(tb, change_cols, "year")

    tb = tb.drop(columns=["total_population", "male_population", "female_population"])

    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_do, tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def migrant_change_5_years(tb, tb_row, col_name):
    cnt = tb_row["country"]
    yr = tb_row["year"]
    if yr == 1990:
        return pd.NA
    else:
        tb_prev = tb[(tb["country"] == cnt) & (tb["year"] == yr - 5)].iloc[0]
        if tb_prev.empty:
            return pd.NA
        else:
            return float(tb_row[col_name] - tb_prev[col_name])


def format_table(tb, country_cols, value_name, value_factor=1):
    """Formats tables from UN DESA data to have consistent country, year, and value columns by:
    - Melting the table to have a year column
    - Cleaning year & country column to have consistent values
    - Pivoting table to have one row per country and year and values for sex in extra columns
    - Scaling values by thousands where necessary
    - Adding metadata to the table
    - Harmonizing column and country names
    """
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

    tb[value_name] = pd.to_numeric(tb[value_name], errors="coerce")
    tb = tb.pivot_table(index=country_cols + ["year"], columns="sex", values=value_name).reset_index()

    # scale values where necessary
    for col in ["all sexes", "females", "males"]:
        tb[col] = tb[col] * value_factor

    # add metadata
    tb = add_metadata(tb, ["all sexes", "females", "males"], "year")

    tb = tb.rename(
        columns={
            "all sexes": f"{value_name}_all_sexes",
            "females": f"{value_name}_female",
            "males": f"{value_name}_male",
        }
    )

    for cnt in country_cols:
        tb = geo.harmonize_countries(
            df=tb, countries_file=paths.country_mapping_path, country_col=cnt, warn_on_unused_countries=False
        )

    # remove duplicate data
    tb = tb.drop_duplicates()

    return tb


def add_metadata(tb: Table, cols_wo_metadata: list, col_with_metadata: str):
    for col in cols_wo_metadata:
        tb[col] = tb[col].copy_metadata(tb[col_with_metadata])
    return tb


def year_to_sex(year):
    if year in BOTH_SEXES:
        return "all sexes"
    elif year in MALES:
        return "males"
    elif year in FEMALES:
        return "females"
