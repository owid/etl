"""Load a meadow dataset and create a garden dataset."""

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_migrant_stock")

    # Read table from meadow dataset.
    tb = ds_meadow["un_migrant_stock_dest_origin"].reset_index()

    # pivot year columns to rows
    tb = tb.melt(
        id_vars=["country_destination", "country_origin", "data_type"],
        value_vars=ALL_YEARS,
        var_name="year",
        value_name="people",
    )

    # add sex column
    tb["sex"] = tb["year"].apply(lambda x: year_to_sex(x))

    # clean year column
    tb["year"] = tb["year"].str.split("_").apply(lambda x: int(x[1]))

    # clean country columns
    tb["country_destination"] = tb["country_destination"].str.strip()
    tb["country_origin"] = tb["country_origin"].str.strip()

    # Remove aggregated regions from the dataset.
    tb = tb[~tb["country_destination"].isin(REGIONS)]
    tb = tb[~tb["country_origin"].isin(REGIONS)]

    # Pivot table to have one row per country pair and year.
    tb = tb.pivot_table(
        index=["country_destination", "country_origin", "year"], columns="sex", values="people"
    ).reset_index()

    # add metadata
    for col in ["all sexes", "females", "males"]:
        tb[col] = tb[col].copy_metadata(tb["year"])

    # rename columns
    tb = tb.rename(columns={"all sexes": "migrants_all_sexes", "females": "migrants_female", "males": "migrants_male"})

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="country_destination")
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="country_origin")

    tb = tb.format(["country_destination", "country_origin", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def year_to_sex(year):
    if year in BOTH_SEXES:
        return "all sexes"
    elif year in MALES:
        return "males"
    elif year in FEMALES:
        return "females"
