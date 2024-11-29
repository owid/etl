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

    # Life tables
    tb_lt = standardize_sex_cat_names(tb_lt)
    tb_lt = tb_lt.sort_values("format").drop_duplicates(
        subset=[col for col in tb_lt.columns if col != "format"], keep="first"
    )

    ## Check
    summary = tb_lt.groupby(["country", "year", "sex", "type", "age"], as_index=False).size().sort_values("size")
    num_dups = summary.loc[summary["size"] != 1].shape[0]
    assert num_dups <= 19
    ## Final drops
    tb_lt = tb_lt.loc[~(tb_lt["format"] == "5x1") & (tb_lt["age"] == "110+")]

    # Exposures
    tb_exp = standardize_sex_cat_names(tb_exp, {"female", "male", "total"})

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
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
