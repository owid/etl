"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from shared import (
    LAST_YEAR,
    add_latest_years_with_constant_num_countries,
    add_population_to_table,
    fill_timeseries,
    init_table_countries_in_region,
)
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Only for table tb_regions:
# The current list of members goes until 2016, we artificially extend it until year of latest 31st of December
EXPECTED_LAST_YEAR = 2016
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("isd")
    # Load population table
    ds_pop = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["isd"].reset_index()

    #
    # Process data.
    #
    log.info("isd: harmonizing countries")
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        country_col="statename",
    )

    # Fixes
    log.info("isd: fixing data")
    tb = fix_data(tb)

    # Format table
    tb_formatted = format_table(tb)

    # Create new table
    log.info("isd: creating table with countries in region")
    tb_regions = create_table_countries_in_region(tb=tb_formatted)

    # Population table
    tb_pop = add_population_to_table(tb_formatted, ds_pop, country_col="statename", region_alt=True)

    # Combine tables
    tb_regions = tb_regions.merge(tb_pop, how="left", on=["region", "year"])

    # Get table with id, year, country (whenever that country was present)
    tb_countries = create_table_country_years(tb_formatted)

    # Add to tables list
    tables = [
        tb.set_index(["cownum", "start", "end"], verify_integrity=True).sort_index(),
        tb_regions.set_index(["region", "year"], verify_integrity=True).sort_index(),
        tb_countries.set_index(["id", "year"], verify_integrity=True).sort_index(),
    ]

    # tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def fix_data(tb: Table) -> Table:
    """Fix ISD data.

    Original ISD data has some issues:

        - Wrong date (31st of September)
        - Some cownums are mapped to more than one country.

    NOTE: This has been reported to the source
    """
    # Fixes wrong date '31-09-1896' -> '30-09-1896' (there is no 31st Sep)
    # I've reported this to the source
    tb["end"] = tb["end"].astype(str)
    tb.loc[tb["end"] == "31-09-1896", "end"] = "30-09-1896"

    # Fix COW Num
    ## 7589 is mapped to "Bharatpur" and "Chamba"
    ## 7590 is mapped to "Cutch" and "Singhbhum"
    ## 8542 is mapped to "Karangasem" and "Mataram Lombok"
    ## Checking the docs of v1 (v2 does not provide IDs): https://static1.squarespace.com/static/54eccfa0e4b08d8eee5174af/t/54ede030e4b0a0f14faa8f84/1424875568381/ISD+Codebook_version1.pdf, it seems that the correct mapping is:
    ## But also reaching out to Ryan & co. via mail to confirm. They provided the correct mapping (will be fixed in upcoming v3)
    ##
    ## Bharatpur = 7589  (already good)
    ## Chamba = 7638
    tb.loc[(tb["cownum"] == 7589) & (tb["cowid"] == "CHM"), "cownum"] = 7638
    ## Cutch = 7590 (already good)
    ## Singhbhum = 7568
    tb.loc[(tb["cownum"] == 7590) & (tb["cowid"] == "SNB"), "cownum"] = 7568
    ## Karangasem = 8541
    tb.loc[(tb["cownum"] == 8542) & (tb["cowid"] == "KRG"), "cownum"] = 8541
    ## Mataram Lombok = 8542
    tb.loc[(tb["cownum"] == 8542) & (tb["cowid"] == "LOM"), "cownum"] = 8542
    return tb


def format_table(tb: Table) -> Table:
    """Format table.

    - Create years
    - Expand observations
    - Map countries to regions
    """
    tb = init_table_countries_in_region(
        tb,
        date_format="%d-%m-%Y",
        column_start="start",
        column_end="end",
        column_id="cownum",
        column_country="statename",
    )

    # Get region name
    tb["region"] = tb["cownum"].apply(code_to_region)

    return tb


def create_table_countries_in_region(tb: Table) -> Table:
    """Create table with number of countries in each region per year."""
    # Get number of countries per region per year
    tb_regions = (
        tb.groupby(["region", "year"], as_index=False)
        .agg({"cownum": "nunique"})
        .rename(columns={"cownum": "number_countries"})
    )

    # Add other regions to the table (possible overlapping with the existing ones)
    # The code should be checked again, as it might be outdated and not work
    #
    # Same as before, but with an alternate region set. Basically, instead of (Africa, Middle East) -> (Sub-Saharan Africa, North Africa & Middle East)
    tb["region_alt"] = tb["cownum"].apply(code_to_region_alt)
    tb_regions_alt = (
        tb.groupby(["region_alt", "year"], as_index=False)
        .agg({"cownum": "nunique"})
        .rename(columns={"cownum": "number_countries"})
    ).rename(columns={"region_alt": "region"})
    tb_regions_alt = tb_regions_alt[tb_regions_alt["region"] != "Rest"]

    tb_regions = pr.concat([tb_regions, tb_regions_alt], ignore_index=True)

    # Sanity check
    cols = ["year", "number_countries"]
    sum_1 = (
        tb_regions.loc[tb_regions["region"].isin(["Middle East", "Africa"]), cols]
        .groupby("year")["number_countries"]
        .sum()
    )
    sum_2 = (
        tb_regions.loc[tb_regions["region"].isin(["North Africa and the Middle East", "Sub-Saharan Africa"]), cols]
        .groupby("year")["number_countries"]
        .sum()
    )
    assert (
        sum_1 == sum_2
    ).all(), f"The following equation should hold: ME + AFR = NAME + SSA, but {sum_1[sum_1 != sum_2]}"

    # Get numbers for World
    tb_world = (
        tb.groupby(["year"], as_index=False).agg({"cownum": "nunique"}).rename(columns={"cownum": "number_countries"})
    )
    tb_world["region"] = "World"

    # Combine
    tb_regions = pr.concat([tb_regions, tb_world], ignore_index=True, short_name="isd_regions")

    # Finish by adding missing last years
    tb_regions = add_latest_years_with_constant_num_countries(
        tb_regions,
        column_year="year",
        expected_last_year=EXPECTED_LAST_YEAR,
    )

    return tb_regions


def code_to_region(cow_code: int) -> str:
    """Convert code to region name."""
    match cow_code:
        case c if 2 <= c <= 165:
            return "Americas"
        case c if (200 <= c <= 395) or (c in [2558, 3375]):
            return "Europe"
        case c if (402 <= c <= 626) or (4044 <= c <= 6257):
            return "Africa"
        case c if (630 <= c <= 698) or (6821 <= c <= 6845):
            return "Middle East"
        case c if (700 <= c <= 990) or (7003 <= c <= 9210):
            return "Asia and Oceania"
        case _:
            raise ValueError(f"Invalid ISD code: {cow_code}")


def code_to_region_alt(cow_code: int) -> str:
    """Convert code to (alternative) region name.

    Adds regions that might overlap with the existing ones.
    """
    match cow_code:
        case c if (
            (402 <= c <= 434)
            or (437 <= c <= 482)  # Skipping Mauritania, Niger
            or (484 <= c <= 591)  # Skipping Chad
            or (4044 <= c <= 4343)  # SKipping: Morocco, Algeria, Tunisia, Libya, Sudan, South Sudan
            or (4362 <= c <= 4761)  # Skipping Brakna, Trarza Emirate
            or (4765 <= c <= 4831)  # Skipping Kanem-Bornu
            or (4841 <= c <= 5814)  # Skipping Wadai  # Skipping Darfur, Funj Sultanate, Shilluk Kingdom, Tegali Kingdom
        ):
            return "Sub-Saharan Africa"
        case c if (
            (c in [435, 436, 483])  # North Africa
            or (600 <= c <= 698)  # NA & Middle East
            or (c in [4352, 4354, 4763, 4832])  # NA
            or (6251 <= c <= 6845)  # NA & ME
        ):
            return "North Africa and the Middle East"
        case _:
            return "Rest"


def create_table_country_years(tb: Table) -> Table:
    """Create table with each country present in a year."""
    tb_countries = (
        tb[["cownum", "year", "statename"]]
        .copy()
        .rename(
            columns={
                "cownum": "id",
                "statename": "country",
            }
        )
    )

    # define mask for last year
    mask = tb_countries["year"] == EXPECTED_LAST_YEAR

    tb_last = fill_timeseries(
        tb_countries[mask].drop(columns="year"),
        EXPECTED_LAST_YEAR + 1,
        LAST_YEAR,
    )

    tb = pr.concat([tb_countries, tb_last], ignore_index=True, short_name="isd_countries")

    # Fix country names
    ## Serbia and Montenegro, Serbia
    tb["country"] = tb["country"].astype(str)
    # tb.loc[(tb["id"] == 345) & (tb["year"] >= 1992) & (tb["year"] < 2006), "country"] = "Serbia and Montenegro"
    # tb.loc[(tb["id"] == 345) & (tb["year"] >= 2006), "country"] = "Serbia"
    ## Replace Yugoslavia -> Serbia
    tb_countries["country"] = tb_countries["country"].replace({"Yugoslavia": "Serbia"})

    return tb
