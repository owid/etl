"""Load a meadow dataset and create a garden dataset."""

import os

from shared import add_regional_aggregates, add_share_population

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

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

# Use this to process subset of data. Set list of causes separated by comma. For example:
# SUBSET='Zika virus' etlr ihme_gbd/2024-05-20/gbd_prevalence --private --grapher
# This command will upsert only data for Zika virus to MySQL.
SUBSET = os.environ.get("SUBSET")


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_prevalence")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_prevalence"].reset_index()
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # If subset is defined, filter the data to given causes.
    if SUBSET:
        tb = tb[tb.cause.isin(SUBSET.split(","))]

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = add_regional_aggregates(
        tb,
        ds_regions,
        index_cols=["country", "year", "metric", "measure", "cause", "age"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
    )

    # Add a share of the population column
    tb = add_share_population(tb)

    # Split into two tables: one for prevalence, one for incidence
    tb_prevalence = tb[tb["measure"] == "Prevalence"].copy()
    tb_incidence = tb[tb["measure"] == "Incidence"].copy()

    # Drop the measure column
    tb_prevalence = tb_prevalence.drop(columns="measure")
    tb_incidence = tb_incidence.drop(columns="measure")

    # Format the tables
    tb_prevalence = tb_prevalence.format(["country", "year", "metric", "age", "cause"], short_name="gbd_prevalence")
    tb_incidence = tb_incidence.format(["country", "year", "metric", "age", "cause"], short_name="gbd_incidence")

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_prevalence, tb_incidence],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
