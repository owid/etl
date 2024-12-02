"""Load a meadow dataset and create a garden dataset."""

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
    # 1/ Life tables
    def _sanity_check_lt(tb):
        summary = tb.groupby(["country", "year", "sex", "type", "age"], as_index=False).size().sort_values("size")
        row_dups = summary.loc[summary["size"] != 1]
        assert row_dups.shape[0] <= 19, "Found duplicated rows in life tables!"
        assert (row_dups["country"].unique() == "Switzerland").all() & (
            row_dups["year"] <= 1931
        ).all(), "Unexpected duplicates in life tables!"
        tb = tb.loc[~(tb["format"] == "5x1") & (tb["age"] == "110+")]
        return tb

    tb_lt = reshape_table(
        tb=tb_lt,
        col_index=["country", "year", "sex", "age", "type"],
        sex_expected={"females", "males", "total"},
        callback_post=_sanity_check_lt,
    )

    # 2/ Exposures
    tb_exp = reshape_table(
        tb=tb_exp,
        col_index=["country", "year", "sex", "age", "type"],
    )

    # 3/ Mortality
    tb_mort = reshape_table(
        tb=tb_mort,
        col_index=["country", "year", "sex", "age", "type"],
    )

    # 4/ Population
    tb_pop = reshape_table(
        tb=tb_pop,
        col_index=["country", "year", "sex", "age"],
    )

    # 5/ Births
    tb_births = reshape_table(
        tb=tb_births,
        col_index=["country", "year", "sex"],
    )

    # tb = geo.harmonize_countries(
    #     df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    # )

    tables = [
        tb_lt.format(["country", "year", "sex", "age", "type"]),
        tb_exp.format(["country", "year", "sex", "age", "type"]),
        tb_mort.format(["country", "year", "sex", "age", "type"]),
        tb_pop.format(["country", "year", "sex", "age"]),
        tb_births.format(["country", "year", "sex"]),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def reshape_table(tb, col_index, sex_expected=None, callback_post=None):
    """Reshape a table.

    Input table has column `format`, which is sort-of redundant. This function ensures we can safely drop it (i.e. no duplicate rows).

    Additionally, it standardizes the dimension values.
    """
    if sex_expected is None:
        sex_expected = {"female", "male", "total"}

    # Standardize dimension values
    tb = standardize_sex_cat_names(tb, sex_expected)

    # Drop duplicate rows
    tb = tb.sort_values("format").drop_duplicates(subset=[col for col in tb.columns if col != "format"], keep="first")

    # Check no duplicates
    summary = tb.groupby(col_index, as_index=False).size().sort_values("size")
    row_dups = summary.loc[summary["size"] != 1]
    if callback_post is not None:
        tb = callback_post(tb)
    else:
        summary = tb.groupby(col_index, as_index=False).size().sort_values("size")
        row_dups = summary.loc[summary["size"] != 1]
        assert row_dups.empty, "Found duplicated rows in life tables!"

    # Final dropping o f columns
    tb = tb.drop(columns="format")

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
