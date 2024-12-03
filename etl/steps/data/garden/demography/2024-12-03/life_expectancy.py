"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Year of last estimate
YEAR_ESTIMATE_LAST = 2023
YEAR_WPP_START = 1950

# Region mapping
# We will be using continent names without (Entity) suffix. This way charts show continuity between lines from different datasets (e.g. riley and UN)
REGION_MAPPING = {
    "Africa (Riley 2005)": "Africa",
    "Americas (Riley 2005)": "Americas",
    "Asia (Riley 2005)": "Asia",
    "Europe (Riley 2005)": "Europe",
    "Oceania (Riley 2005)": "Oceania",
    "Africa (UN)": "Africa",
    "Northern America (UN)": "Northern America",
    "Latin America and the Caribbean (UN)": "Latin America and the Caribbean",
    "Asia (UN)": "Asia",
    "Europe (UN)": "Europe",
    "Oceania (UN)": "Oceania",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    ## Life tables
    paths.log.info("reading dataset `life_tables`")
    ds_lt = paths.load_dataset("life_tables")
    tb_lt = ds_lt.read("life_tables")
    ## zijdeman_et_al_2015
    paths.log.info("reading dataset `zijdeman_et_al_2015`")
    ds_zi = paths.load_dataset("zijdeman_et_al_2015")
    tb_zi = ds_zi.read("zijdeman_et_al_2015")
    ## Riley
    paths.log.info("reading dataset `riley_2005`")
    ds_ri = paths.load_dataset("riley_2005")
    tb_ri = ds_ri.read("riley_2005")
    ## WPP
    paths.log.info("reading dataset `un_wpp`")
    ds_un = paths.load_dataset("un_wpp")
    tb_un = ds_un.read("life_expectancy")

    #
    # Process data.
    #
    paths.log.info("processing data")
    tb_lt = process_lt(tb_lt)
    tb_un = process_un(tb_un)
    tb_zi = process_zi(tb_zi)
    tb_ri = process_ri(tb_ri)

    paths.log.info("combining tables")
    tb = combine_tables(tb_lt, tb_un, tb_zi, tb_ri)

    # Rename regions, and use column 'country' instead of 'country'
    tb["country"] = tb["country"].replace(REGION_MAPPING)

    # Add Americas
    # tb = add_americas(tb, ds_un)

    ## Check values
    paths.log.info("final checks")
    _check_column_values(tb, "sex", {"total", "male", "female"})
    _check_column_values(tb, "age", {0, 10, 15, 25, 45, 65, 80})

    # Create three tables: (i) only historical values, (ii) only future values, (iii) all values
    columns_index = ["country", "year", "sex", "age"]

    ## (i) Main table (historical values)
    tb_main = tb.loc[tb["year"] <= YEAR_ESTIMATE_LAST].copy()

    ## (ii) Only projections
    tb_only_proj = tb.loc[tb["year"] > YEAR_ESTIMATE_LAST].copy()
    tb_only_proj = _add_suffix_to_indicators(tb_only_proj, "_only_proj", columns_index=columns_index)
    ## Table only with projections should only contain UN as origin
    origins_un = [origin for origin in tb_main["life_expectancy"].m.origins if origin.producer == "United Nations"]
    for col in tb_only_proj.columns:
        tb_only_proj[col].origins = origins_un

    ## (iii) All values
    tb_with_proj = tb.copy()
    # Only preserve ages that have projections (i.e. data after YEAR_ESTIMATE_LAST)
    ages_with_projections = set(tb_with_proj.loc[tb_with_proj["year"] > YEAR_ESTIMATE_LAST, "age"].unique())
    tb_with_proj = tb_with_proj.loc[tb_with_proj["age"].isin(ages_with_projections)]
    # Column names
    tb_with_proj = _add_suffix_to_indicators(tb_with_proj, "_with_proj", columns_index=columns_index)

    # Format
    tables = [
        tb_main.format(columns_index, short_name=paths.short_name),
        tb_only_proj.format(columns_index, short_name=f"{paths.short_name}_only_proj"),
        tb_with_proj.format(columns_index, short_name=f"{paths.short_name}_with_proj"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_lt.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def _add_suffix_to_indicators(tb, suffix, columns_index=None):
    if columns_index is None:
        columns_index = []
    tb.columns = [f"{col}{suffix}" if col not in columns_index else col for col in tb.columns]
    return tb


def process_lt(tb: Table) -> Table:
    """Process LT data and output it in the desired format.

    Desired format is with columns country, year, sex, age | life_expectancy.
    """
    tb = tb.loc[
        (tb["age"].isin(["0", "10", "15", "25", "45", "65", "80"])) & (tb["type"] == "period"),
        ["country", "year", "sex", "age", "life_expectancy"],
    ]

    # Assign dtype
    tb["age"] = tb["age"].astype("Int64")

    # Update life_expectancy values
    tb["life_expectancy"] = tb["life_expectancy"] + tb["age"]

    # Check latest year
    assert (
        tb["year"].max() == YEAR_ESTIMATE_LAST
    ), f"Last year was {tb['year'].max()}, but should be {YEAR_ESTIMATE_LAST}"

    # Check column values
    ## sex
    _check_column_values(tb, "sex", {"total", "female", "male"})
    ## age
    _check_column_values(tb, "age", {0, 10, 15, 25, 45, 65, 80})

    return tb


def process_un(tb: Table) -> Table:
    """Process UN WPP data and output it in the desired format.

    Desired format is with columns country, year, sex, age | life_expectancy.
    """
    # Sanity check
    assert (
        tb["year"].min() == YEAR_WPP_START
    ), f"Year of first estimate is different than {YEAR_WPP_START}, it is {tb['year'].min()}"

    # Filter
    ## dimension values: metric=life_expectancy, variant=medium, year >= YEAR_ESTIMATE_LAST
    ## columns: country, year, value, sex, age
    tb = tb.loc[
        (tb["year"] > YEAR_ESTIMATE_LAST) & (tb["variant"] == "medium"),
        ["country", "year", "sex", "age", "life_expectancy"],
    ]

    # Rename column values
    tb["age"] = tb["age"].replace({"at birth": "0"}).astype("Int64")
    tb["sex"] = tb["sex"].replace({"all": "total"})

    # Check column values
    ## sex
    _check_column_values(tb, "sex", {"total", "female", "male"})
    ## age
    _check_column_values(tb, "age", {0, 15, 65, 80})

    # Check minimum year
    assert (
        tb.groupby("country", observed=True).year.min() == YEAR_ESTIMATE_LAST + 1
    ).all(), f"Some entry with latest year different than {YEAR_ESTIMATE_LAST}"

    return tb


def process_zi(tb: Table) -> Table:
    """Process Zijdeman data and output it in the desired format.

    Desired format is with columns country, year, sex, age | life_expectancy.
    """
    # Filter
    ## dimension values: metric=life_expectancy, variant=medium, year >= YEAR_ESTIMATE_LAST
    ## columns: country, year, value, sex, age
    tb = tb.loc[(tb["year"] <= YEAR_ESTIMATE_LAST)]

    # Add columns
    # tb["type"] = "period"
    tb["age"] = 0
    tb["sex"] = "total"

    # Resolution
    tb["life_expectancy"] = tb["life_expectancy"].astype("Float64").round(3)

    # Dtypes
    tb = tb.astype(
        {
            "age": "Int64",
            "sex": "string",
        }
    )

    # Sanity check
    assert tb["year"].max() == 2012, f"Last year was {tb['year'].max()}, but should be 2012"

    return tb


def process_ri(tb: Table) -> Table:
    """Process Riley data and output it in the desired format.

    Desired format is with columns country, year, sex, age | life_expectancy.
    """
    # Filter
    ## dimension values: metric=life_expectancy, variant=medium, year >= YEAR_ESTIMATE_LAST
    ## columns: country, year, value, sex, age
    tb = tb.loc[(tb["year"] < 1950),]

    # Rename column names
    tb = tb.rename(columns={"entity": "country"})

    # Add columns
    # tb["type"] = "period"
    tb["sex"] = "total"
    tb["age"] = 0

    # Dtypes
    tb = tb.astype(
        {
            "age": "Int64",
            "sex": "string",
        }
    )

    # Resolution
    tb["life_expectancy"] = tb["life_expectancy"].astype("Float64").round(3)

    return tb


def combine_tables(tb_lt: Table, tb_un: Table, tb_zi: Table, tb_ri: Table) -> Table:
    """Combine all LE tables.

    - Only HMD (within LT) contains cohort data.
    - LE broken down by sex and age is available from LT and UN_WPP.
    - LT already contains UN_WPP data, but without projections. That's why we also use UN WPP's
    - RIL and ZIJ contain figures for all sexes and at birth. Only period.
    """
    tb = pr.concat([tb_lt, tb_un], ignore_index=True, short_name="life_expectancy")

    # Separate LE at birth from at different ages
    mask = (tb["age"] == 0) & (tb["sex"] == "total")
    tb_0 = tb.loc[mask]
    tb = tb.loc[~mask]

    # Extend tb_0 (only for period)
    ## Zijdeman: complement country data
    tb_0 = tb_0.merge(tb_zi, how="outer", on=["country", "year", "sex", "age"], suffixes=("", "_zij"))
    tb_0["life_expectancy"] = tb_0["life_expectancy"].fillna(tb_0["life_expectancy_zij"])
    tb_0 = tb_0.drop(columns=["life_expectancy_zij"])
    ## Riley: complement with continent data
    tb_0 = pr.concat([tb_0, tb_ri], ignore_index=True)

    # Combine tb_0 with tb
    tb = tb.merge(tb_0, on=["country", "year", "sex", "age"], how="outer", suffixes=("", "_0"))

    # For some reason, 'sex' is assigned type object
    tb["sex"] = tb["sex"].astype("string")

    return tb


def _check_column_values(tb: Table, column: str, expected_values: set) -> None:
    """Check that a column has only expected values."""
    unexpected_values = set(tb[column]) - expected_values
    assert not unexpected_values, f"Unexpected values found in column {column}: {unexpected_values}"


def add_americas(tb: Table, ds_population: Dataset) -> Table:
    """Estimate value for the Americas using North America and LATAM/Caribbean.

    Only performs this estimation for:

        sex = all
        age = 0

    It estimates it by doing the population-weighted average of life expectancies.
    """
    # filter only member countries of the region
    AMERICAS_MEMBERS = ["Northern America", "Latin America and the Caribbean"]
    tb_am = tb.loc[(tb["country"].isin(AMERICAS_MEMBERS)) & (tb["sex"] == "total") & (tb["age"] == 0),].copy()

    # sanity check
    assert (
        tb_am.groupby(["country", "year"]).size().max() == 1
    ), "There is more than one entry for a (country, year) tuple!"

    # add population for LATAM and Northern America (from WPP, hence since 1950)
    assert tb_am["year"].min() == YEAR_WPP_START
    tb_am = add_population_americas_from_wpp(tb_am, ds_population)

    # sanity check: ensure there are NO missing values. This way, we can safely do the groupby
    assert (tb_am[["life_expectancy_0", "population"]].isna().sum() == 0).all()

    # estimate values for regions
    # y(country) = weight(country) * metric(country)
    tb_am["life_expectancy_0"] *= tb_am["population"]

    # z(region) = sum{ y(country) } for country in region
    tb_am = tb_am.groupby("year", as_index=False)[["life_expectancy_0", "population"]].sum()

    # z(region) /  sum{ population(country) } for country in region
    tb_am["life_expectancy_0"] /= tb_am["population"]

    # assign region name
    tb_am = tb_am.assign(
        country="Americas",
        sex="total",
        age=0,
    )

    # drop unused column
    tb_am = tb_am.drop(columns="population")

    # concatenate
    tb = pr.concat([tb, tb_am], ignore_index=True)
    return tb


def add_population_americas_from_wpp(tb: Table, ds_population: Dataset) -> Table:
    """Add population values for LATAM and Northern America.

    Data is sourced from UN WPP, hence only available since 1950.
    """
    pop = load_america_population_from_unwpp(ds_population)
    tb = tb.merge(pop, on=["country", "year"])
    return tb


def load_america_population_from_unwpp(ds_population: Dataset) -> Table:
    """Load population data from UN WPP for Northern America and Latin America and the Caribbean.

    We use this dataset instead of the long-run because we want the entities as defined by the UN.
    """
    # load population from WPP
    countries = ["Latin America and the Caribbean (UN)", "Northern America (UN)"]
    tb = ds_population.read("population")
    tb = tb.loc[
        (tb["country"].isin(countries))
        & (tb["sex"] == "all")
        & (tb["age"] == "all")
        & (tb["variant"].isin(["estimates", "medium"])),
        ["country", "year", "population"],
    ]
    assert len(set(tb["country"])) == 2, f"Check that all of {countries} are in df"
    tb["country"] = tb["country"].replace(REGION_MAPPING).drop(columns="country")

    return tb
