"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    paths.log.info("reading tables")
    tb_lt = ds_meadow.read("life_tables")
    tb_exp = ds_meadow.read("exposures")
    tb_mort = ds_meadow.read("deaths")
    tb_pop = ds_meadow.read("population")
    tb_births = ds_meadow.read("births")

    #
    # Process data.
    #
    paths.log.info("processing tables")

    # 1/ Life tables
    def _sanity_check_lt(tb):
        summary = tb.groupby(["country", "year", "sex", "type", "age"], as_index=False).size().sort_values("size")
        row_dups = summary.loc[summary["size"] != 1]
        assert row_dups.shape[0] <= 19, "Found duplicated rows in life tables!"
        assert (row_dups["country"].unique() == "Switzerland").all() & (
            row_dups["year"] <= 1931
        ).all(), "Unexpected duplicates in life tables!"
        tb = tb.loc[~(tb["format"] == "5x1") & (tb["age"] == "110+")]

        flag = (
            (tb_lt["country"] == "Switzerland")
            & (tb_lt["age"] == "110+")
            & (tb_lt["type"] == "cohort")
            & (tb_lt["sex"] == "Males")
            & (tb_lt["year"] <= 1931)
            & (tb_lt["year"] >= 1913)
        )
        tb = tb.loc[~flag]

        return tb

    tb_lt = process_table(
        tb=tb_lt,
        col_index=["country", "year", "sex", "age", "type"],
        sex_expected={"females", "males", "total"},
        callback_post=_sanity_check_lt,
    )
    # Scale central death rates
    tb_lt["central_death_rate"] = tb_lt["central_death_rate"] * 1_000
    tb_lt["probability_of_death"] = tb_lt["probability_of_death"] * 100

    # 2/ Exposures
    tb_exp = process_table(
        tb=tb_exp,
        col_index=["country", "year", "sex", "age", "type"],
    )

    # 3/ Mortality
    tb_mort = process_table(
        tb=tb_mort,
        col_index=["country", "year", "sex", "age", "type"],
    )
    assert set(tb_mort["type"].unique()) == {"period"}, "Unexpected values in column 'type' in mortality tables!"
    tb_mort = tb_mort.drop(columns="type")

    # 4/ Population
    tb_pop = process_table(
        tb=tb_pop,
        col_index=["country", "year", "sex", "age"],
    )

    # 5/ Births
    tb_births = process_table(
        tb=tb_births,
        col_index=["country", "year", "sex"],
    )
    tb_pop_agg = tb_pop.groupby(["country", "year", "sex"], as_index=False)["population"].sum()
    tb_births = tb_births.merge(tb_pop_agg, on=["country", "year", "sex"], how="left")
    tb_births["birth_rate"] = tb_births["births"] / tb_births["population"] * 1_000
    tb_births = tb_births.drop(columns=["population"])

    # 6/ Create table with differences and ratios
    tb_ratios = make_table_diffs_ratios(tb_lt)

    # Create list with tables
    paths.log.info("saving tables")
    tables = [
        tb_lt.format(["country", "year", "sex", "age", "type"]),
        tb_exp.format(["country", "year", "sex", "age", "type"]),
        tb_mort.format(["country", "year", "sex", "age"]),
        tb_pop.format(["country", "year", "sex", "age"]),
        tb_births.format(["country", "year", "sex"]),
        tb_ratios.format(["country", "year", "age", "type"], short_name="diff_ratios"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_table(tb, col_index, sex_expected=None, callback_post=None):
    """Reshape a table.

    Input table has column `format`, which is sort-of redundant. This function ensures we can safely drop it (i.e. no duplicate rows).

    Additionally, it standardizes the dimension values.
    """
    paths.log.info(f"processing table {tb.name}")

    if sex_expected is None:
        sex_expected = {"female", "male", "total"}

    # Standardize dimension values
    tb = standardize_sex_cat_names(tb, sex_expected)

    # Drop duplicate rows
    tb = tb.sort_values("format").drop_duplicates(subset=[col for col in tb.columns if col != "format"], keep="first")

    # Check no duplicates
    if callback_post is not None:
        tb = callback_post(tb)
    else:
        summary = tb.groupby(col_index, as_index=False).size().sort_values("size")
        row_dups = summary.loc[summary["size"] != 1]
        assert row_dups.empty, "Found duplicated rows in life tables!"

    # Final dropping o f columns
    tb = tb.drop(columns="format")

    # Country name standardization
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Make year column integer
    tb["year"] = tb["year"].astype(int)

    return tb


def standardize_sex_cat_names(tb, sex_expected):
    # Define expected sex categories
    sex_expected = {s.lower() for s in sex_expected}

    # Set sex categories to lowercase
    tb["sex"] = tb["sex"].str.lower()

    # Sanity check categories
    sex_found = set(tb["sex"].unique())
    assert sex_found == sex_expected, f"Unexpected sex categories! Found {sex_found} but expected {sex_expected}"

    # Rename
    tb["sex"] = tb["sex"].replace({"females": "female", "males": "male"})

    return tb


def make_table_diffs_ratios(tb: Table) -> Table:
    """Create table with metric differences and ratios.

    Currently, we estimate:

    - female - male: Life expectancy
    - male/female: Life Expectancy, Central Death Rate
    """
    # Pivot & obtain differences and ratios
    cols_index = ["country", "year", "age", "type"]
    tb_new = (
        tb.pivot_table(
            index=cols_index,
            columns="sex",
            values=["life_expectancy", "central_death_rate"],
        )
        .assign(
            life_expectancy_fm_diff=lambda df: df[("life_expectancy", "female")] - df[("life_expectancy", "male")],
            life_expectancy_mf_ratio=lambda df: df[("life_expectancy", "male")] / df[("life_expectancy", "female")],
            central_death_rate_mf_ratio=lambda df: df[("central_death_rate", "male")]
            / df[("central_death_rate", "female")],
        )
        .reset_index()
    )

    # Keep relevant columns
    cols = [col for col in tb_new.columns if col[1] == ""]
    tb_new = tb_new.loc[:, cols]

    # Rename columns
    tb_new.columns = [col[0] for col in tb_new.columns]

    # Add metadata back
    for col in tb_new.columns:
        if col not in cols_index:
            tb_new[col] = tb_new[col].copy_metadata(tb["life_expectancy"])

    return tb_new
