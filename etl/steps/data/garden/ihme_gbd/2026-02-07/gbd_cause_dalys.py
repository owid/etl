"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr
from shared import add_regional_aggregates

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
AGE_GROUPS_RANGES = {
    "All ages": [0, None],
    "<5 years": [0, 4],
    "5-14 years": [5, 14],
    "15-49 years": [15, 49],
    "50-69 years": [50, 69],
    "70+ years": [70, None],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_cause_dalys")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_cause_dalys"].reset_index()
    # Drop population_group_name as it only contains 'All Population'
    tb = tb.drop(columns=["population_group_name"])
    ds_regions = paths.load_dataset("regions")
    ds_un_wpp = paths.load_dataset("un_wpp")
    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb=tb)
    # Add regional aggregates
    tb = add_regional_aggregates(
        tb=tb,
        ds_regions=ds_regions,
        ds_un_wpp=ds_un_wpp,
        index_cols=["country", "year", "metric", "measure", "cause", "age"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
        run_percent=True,
    )
    assert all(tb["measure"] == "DALYs (Disability-Adjusted Life Years)")
    # Shorten the metric name for DALYs
    tb["measure"] = "DALYs"

    # add infectious diseases by subtracting maternal and neonatal diseases, and nutritional deficiencies from communicable diseases
    tb = add_infectious_diseases(tb)

    # Drop the measure column
    tb = tb.drop(columns="measure")

    # Format the tables
    tb = tb.format(["country", "year", "metric", "age", "cause"], short_name="gbd_cause_dalys")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_infectious_diseases(tb):
    """
    Separate out communicable diseases from maternal and neonatal diseases, and nutritional deficiencies
    """

    broad_level_group = ["Communicable, maternal, neonatal, and nutritional diseases"]
    maternal_neonatal_nutritional = ["Maternal and neonatal disorders", "Nutritional deficiencies"]

    tb_broad = tb[tb["cause"].isin(broad_level_group)]
    assert len(tb_broad) > 0, "No rows found for 'Communicable, maternal, neonatal, and nutritional diseases'"

    tb_maternal_neonatal_nutritional = tb[tb["cause"].isin(maternal_neonatal_nutritional)]
    assert len(tb_maternal_neonatal_nutritional["cause"].unique()) == len(maternal_neonatal_nutritional), (
        "Not all elements of 'maternal_neonatal_nutritional' are present in tb['cause']"
    )
    tb_maternal_neonatal_nutritional = (
        tb_maternal_neonatal_nutritional.groupby(["country", "age", "metric", "year"], observed=True)["value"]
        .sum()
        .reset_index()
    )

    tb_combine = pr.merge(
        tb_broad,
        tb_maternal_neonatal_nutritional,
        on=["country", "year", "age", "metric"],
        suffixes=("", "_maternal_neonatal_nutritional"),
    )
    tb_infectious = tb_combine.copy()
    tb_infectious["cause"] = "Infectious diseases"
    tb_infectious["value"] = tb_infectious["value"] - tb_infectious["value_maternal_neonatal_nutritional"]
    tb_infectious = tb_infectious.drop(columns="value_maternal_neonatal_nutritional")
    assert all(tb_infectious["value"] >= 0), "Negative values found in 'value' column"

    tb = pr.concat([tb, tb_infectious], ignore_index=True)
    return tb
