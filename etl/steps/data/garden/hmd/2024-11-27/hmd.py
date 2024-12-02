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
    tb_lt = standardize_sex_cat_names(tb_lt)
    tb_lt = tb_lt.sort_values("format").drop_duplicates(
        subset=[col for col in tb_lt.columns if col != "format"], keep="first"
    )
    ## Check
    summary = tb_lt.groupby(["country", "year", "sex", "type", "age"], as_index=False).size().sort_values("size")
    row_dups = summary.loc[summary["size"] != 1]
    assert row_dups.shape[0] <= 19, "Found duplicated rows in life tables!"
    assert (row_dups["country"].unique() == "Switzerland").all() & (
        row_dups["year"] <= 1931
    ).all(), "Unexpected duplicates in life tables!"
    ## Final drops
    tb_lt = tb_lt.loc[~(tb_lt["format"] == "5x1") & (tb_lt["age"] == "110+")]
    tb_lt = tb_lt.drop(columns="format")

    # 2/ Exposures
    tb_exp = standardize_sex_cat_names(tb_exp, {"female", "male", "total"})
    tb_exp = tb_exp.sort_values("format").drop_duplicates(
        subset=[col for col in tb_exp.columns if col != "format"], keep="first"
    )
    ## Check
    summary = tb_exp.groupby(["country", "year", "sex", "type", "age"], as_index=False).size().sort_values("size")
    row_dups = summary.loc[summary["size"] != 1]
    assert row_dups.empty, "Found duplicated rows in life tables!"
    ## Final drops
    tb_exp = tb_exp.drop(columns="format")

    # 3/ Mortality
    tb_mort = standardize_sex_cat_names(tb_mort, {"female", "male", "total"})
    tb_mort = tb_mort.sort_values("format").drop_duplicates(
        subset=[col for col in tb_mort.columns if col != "format"], keep="first"
    )
    ## Check
    summary = tb_mort.groupby(["country", "year", "sex", "type", "age"], as_index=False).size().sort_values("size")
    row_dups = summary.loc[summary["size"] != 1]
    assert row_dups.empty, "Found duplicated rows in life tables!"
    ## Final drops
    tb_mort = tb_mort.drop(columns="format")

    # 4/ Population
    tb_pop = standardize_sex_cat_names(tb_pop, {"female", "male", "total"})
    tb_pop = tb_pop.sort_values("format").drop_duplicates(
        subset=[col for col in tb_pop.columns if col != "format"], keep="first"
    )
    summary = tb_pop.groupby(["country", "year", "sex", "age"], as_index=False).size().sort_values("size")
    row_dups = summary.loc[summary["size"] != 1]
    assert row_dups.empty, "Found duplicated rows in life tables!"
    ## Final drops
    tb_pop = tb_pop.drop(columns="format")

    # 4/ Population
    tb_births = standardize_sex_cat_names(tb_births, {"female", "male", "total"})
    tb_births = tb_births.sort_values("format").drop_duplicates(
        subset=[col for col in tb_births.columns if col != "format"], keep="first"
    )
    summary = tb_births.groupby(["country", "year", "sex"], as_index=False).size().sort_values("size")
    row_dups = summary.loc[summary["size"] != 1]
    assert row_dups.empty, "Found duplicated rows in life tables!"
    ## Final drops
    tb_births = tb_births.drop(columns="format")

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


def standardize_sex_cat_names(tb, sex_expected=None):
    # Define expected sex categories
    if sex_expected is None:
        sex_expected = {"females", "males", "total"}
    else:
        sex = {s.lower() for s in sex_expected}

    # Set sex categories to lowercase
    tb["sex"] = tb["sex"].str.lower()

    # Sanity check categories
    sex_found = set(tb["sex"].unique())
    assert sex_found == sex_expected, f"Unexpected sex categories! Found {sex_found} but expected {sex_expected}"

    # Rename
    tb["sex"] = tb["sex"].replace({"females": "female", "males": "male"})
    return tb
