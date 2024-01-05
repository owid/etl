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
    ds_meadow = paths.load_dataset("population_fariss")
    # Read table from meadow dataset.
    tb = ds_meadow["population_fariss"].reset_index()
    # Load Gleditsch dataset (country codes)
    ds_gw = paths.load_dataset("gleditsch")
    tb_gw = ds_gw["gleditsch_countries"].reset_index()

    # Get code to country table
    tb_gw = get_code_to_country(tb_gw)

    # Keep only `latent_pop` indicator (dataset contains indicators from different sources)
    tb = tb[tb["indicator"] == "latent_pop"]
    # Keep relevant columns
    tb = tb[["gwno", "year", "mean", "sd"]]

    # Get country names
    tb = tb.merge(tb_gw, left_on=["gwno"], right_on=["id"], how="left")
    assert tb["country"].notna().all(), "Missing country names!"

    # Restructure columns
    tb = tb[["country", "year", "mean", "sd"]]

    # Rename columns
    tb = tb.rename(columns={"mean": "population", "sd": "population_sd"}, errors="raise")

    # Scale population (1e4)
    ## For some unknown reason, the source data is scaled by 1e4.
    columns = ["population", "population_sd"]
    tb[columns] *= 1e4

    # Get low and high estimates
    tb["population_low"] = tb["population"] - tb["population_sd"]
    tb["population_high"] = tb["population"] + tb["population_sd"]

    # Set index
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
    """Get code to country table."""
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
