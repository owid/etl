"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Year of last estimate
YEAR_ESTIMATE_LAST = 2021

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
    # Load datasets
    ## Life tables
    paths.log.info("reading dataset `life_tables`")
    ds_lt = paths.load_dataset("life_tables")
    tb_lt = ds_lt["life_tables"].reset_index()
    ## zijdeman_et_al_2015
    paths.log.info("reading dataset `zijdeman_et_al_2015`")
    ds_zi = paths.load_dataset("zijdeman_et_al_2015")
    tb_zi = ds_zi["zijdeman_et_al_2015"].reset_index()
    ## Riley
    paths.log.info("reading dataset `riley_2005`")
    ds_ri = paths.load_dataset("riley_2005")
    tb_ri = ds_ri["riley_2005"].reset_index()
    ## WPP
    paths.log.info("reading dataset `un_wpp`")
    ds_un = paths.load_dataset("un_wpp")
    tb_un = ds_un["un_wpp"].reset_index()

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

    ## Check values
    paths.log.info("final checks")
    _check_column_values(tb, "sex", {"all", "male", "female"})
    _check_column_values(tb, "age", {0, 15, 65, 80})
    _check_column_values(tb, "type", {"period", "cohort"})

    ## Set index
    tb = tb.set_index(["location", "year", "type", "sex", "age"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_lt.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_lt(tb: Table) -> Table:
    """Process LT data and output it in the desired format.

    Desired format is with columns location, year, type, sex, age | life_expectancy.
    """
    tb = tb.loc[
        (tb["age"].isin(["0", "15", "65", "80"])), ["location", "year", "type", "sex", "age", "life_expectancy"]
    ]

    # Assign dtype
    tb["age"] = tb["age"].astype("Int64")

    # Rename column values
    tb["sex"] = tb["sex"].replace({"both": "all"})

    # Update life_expectancy values
    tb["life_expectancy"] = tb["life_expectancy"] + tb["age"]

    # Check latest year
    assert (
        tb["year"].max() == YEAR_ESTIMATE_LAST
    ), f"Last year was {tb['year'].max()}, but should be {YEAR_ESTIMATE_LAST}"

    # Check column values
    ## sex
    _check_column_values(tb, "sex", {"all", "female", "male"})
    ## age
    _check_column_values(tb, "age", {0, 15, 65, 80})
    return tb


def process_un(tb: Table) -> Table:
    """Process UN WPP data and output it in the desired format.

    Desired format is with columns location, year, type, sex, age | life_expectancy.
    """
    # Filter
    ## dimension values: metric=life_expectancy, variant=medium, year >= YEAR_ESTIMATE_LAST
    ## columns: location, year, value, sex, age
    tb = tb.loc[
        (tb["metric"] == "life_expectancy") & (tb["year"] > YEAR_ESTIMATE_LAST) & (tb["variant"] == "medium"),
        ["location", "year", "sex", "age", "value"],
    ]

    # Rename column values
    tb["age"] = tb["age"].replace({"at birth": "0"}).astype("Int64")

    # Rename column names
    tb = tb.rename(
        columns={
            "value": "life_expectancy",
        }
    )

    # Assign type
    tb["type"] = "period"

    # Check column values
    ## sex
    _check_column_values(tb, "sex", {"all", "female", "male"})
    ## age
    _check_column_values(tb, "age", {0, 15, 65, 80})

    # Check minimum year
    assert (
        tb.groupby("location", observed=True).year.min() == YEAR_ESTIMATE_LAST + 1
    ).all(), f"Some entry with latest year different than {YEAR_ESTIMATE_LAST}"

    return tb


def process_zi(tb: Table) -> Table:
    """Process Zijdeman data and output it in the desired format.

    Desired format is with columns location, year, type, sex, age | life_expectancy.
    """
    # Filter
    ## dimension values: metric=life_expectancy, variant=medium, year >= YEAR_ESTIMATE_LAST
    ## columns: location, year, value, sex, age
    tb = tb.loc[(tb["year"] <= YEAR_ESTIMATE_LAST)]

    # Rename column names
    tb = tb.rename(
        columns={
            "country": "location",
        }
    )

    # Add columns
    tb["type"] = "period"
    tb["age"] = 0
    tb["sex"] = "all"

    # Dtypes
    tb = tb.astype({"location": str})

    # Resulution
    tb["life_expectancy"] = tb["life_expectancy"].astype("float64").round(3)

    # Sanity check
    assert tb["year"].max() == 2012, f"Last year was {tb['year'].max()}, but should be 2012"

    return tb


def process_ri(tb: Table) -> Table:
    """Process Riley data and output it in the desired format.

    Desired format is with columns location, year, type, sex, age | life_expectancy.
    """
    # Filter
    ## dimension values: metric=life_expectancy, variant=medium, year >= YEAR_ESTIMATE_LAST
    ## columns: location, year, value, sex, age
    tb = tb.loc[
        (tb["year"] < 1950),
    ]

    # Rename column names
    tb = tb.rename(columns={"entity": "location"})

    # Add columns
    tb["type"] = "period"
    tb["sex"] = "all"
    tb["age"] = 0

    # Resolution
    tb["life_expectancy"] = tb["life_expectancy"].astype("float64").round(1)

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
    tb = tb[tb["age"] != 0]
    tb_0 = tb[tb["age"] == 0]

    # Extend tb_0
    ## Zijdeman: complement country data
    tb_0 = tb_0.merge(tb_zi, how="outer", on=["location", "year", "type", "sex", "age"], suffixes=("", "_zij"))
    tb_0["life_expectancy"] = tb_0["life_expectancy"].fillna(tb_0["life_expectancy_zij"])
    tb_0 = tb_0.drop(columns=["life_expectancy_zij"])
    ## Riley: complement with continent data
    tb_0 = pr.concat([tb_0, tb_ri], ignore_index=True)

    # Combine tb_0 with tb
    tb = tb.merge(tb_0, on=["location", "year", "type", "sex", "age"], how="outer", suffixes=("", "_0"))
    return tb


def _check_column_values(tb: Table, column: str, expected_values: set) -> None:
    """Check that a column has only expected values."""
    unexpected_values = set(tb[column]) - expected_values
    assert not unexpected_values, f"Unexpected values found in column {column}: {unexpected_values}"
