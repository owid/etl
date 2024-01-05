"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RELEVANT_VARS = ["gwno", "year", "mean", "sd"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fariss_et_al")

    # Read table from meadow dataset.
    tb_gdp = ds_meadow["fariss_et_al_gdp"].reset_index()
    tb_gdp_pc = ds_meadow["fariss_et_al_gdp_pc"].reset_index()
    tb_pop = ds_meadow["fariss_et_al_pop"].reset_index()

    # Read Gleditsch country codes
    ds_gleditsch = paths.load_dataset("gleditsch")
    tb_gleditsch = ds_gleditsch["gleditsch_countries"].reset_index()

    #
    # Process data.
    # Select only latent indicator
    tb_gdp = tb_gdp[tb_gdp["indicator"] == "latent_gdp"].reset_index(drop=True)
    tb_gdp_pc = tb_gdp_pc[tb_gdp_pc["indicator"] == "latent_gdppc"].reset_index(drop=True)
    tb_pop = tb_pop[tb_pop["indicator"] == "latent_pop"].reset_index(drop=True)

    # Merge tables
    tb = tb_gdp[RELEVANT_VARS].merge(
        tb_gdp_pc[RELEVANT_VARS],
        how="outer",
        on=["gwno", "year"],
        suffixes=("_gdp", "_gdp_pc"),
        short_name="fariss_et_al",
    )
    tb = tb.merge(tb_pop[RELEVANT_VARS], how="outer", on=["gwno", "year"])

    # Rename mean and sd columns
    tb = tb.rename(columns={"mean": "mean_pop", "sd": "sd_pop"})

    # Get code to country table
    tb_gleditsch = get_code_to_country(tb_gleditsch)

    # Get country names
    tb = tb.merge(tb_gleditsch, left_on=["gwno"], right_on=["id"], how="left")
    assert tb["country"].notna().all(), "Missing country names!"

    # Drop columns
    tb = tb.drop(columns=["id", "gwno"])

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_code_to_country(tb_gw):
    """
    Get code to country table.
    From Lucas' code on `population_fariss`
    """
    # Sanity check: no duplicate country codes
    ## We expect only two codes to have multiple country names assigned: 260 and 580.
    x = tb_gw.groupby("id")["country"].nunique()
    codes = set(x[x > 1].index)
    assert codes == {260, 580}, "Unexpected duplicate country codes!"

    # Fix: Although there were different namings in the past for countries with codes 260 and 580, we want these to have the modern name.
    tb_gw["country"] = tb_gw["country"].replace(
        {
            "Madagascar (Malagasy)": "Madagascar",
            "West Germany": "Germany",
        }
    )

    # Simplify table
    tb_gw = tb_gw[["id", "country"]].drop_duplicates().set_index("id", verify_integrity=True).reset_index()

    return tb_gw
