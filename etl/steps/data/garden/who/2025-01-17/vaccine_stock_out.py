"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
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
    ds_meadow = paths.load_dataset("vaccine_stock_out")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccine_stock_out")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns=["iso_3_code", "who_region", "indcode", "indcatcode", "indcat_description", "indsort"])
    tb = tb.replace({"value": {"ND": pd.NA, "NR": pd.NA}})
    tb = national_stockout_for_any_vaccine(tb)
    tb = district_level_stockout_for_any_vaccine(tb)
    tb = tb.format(["country", "year", "description"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


### Derived metrics


def national_stockout_for_any_vaccine(tb: Table) -> Table:
    """
    Was there a vaccine stockout of any vaccine in the country in the given year?
    """
    # Get the rows relevant to national vaccine stockouts (i.e. not subnational, or stockouts of syringes)

    tb_agg = tb[
        tb["description"].str.contains("Was there a stock-out at the national level")
        & (tb["description"].str.contains("vaccine"))
    ]
    tb_agg = tb_agg.dropna(subset=["value"])
    tb_agg = (
        tb_agg.assign(is_yes=tb_agg["value"].eq("Yes"))
        .groupby(["country", "year"], as_index=False)
        .agg(
            any_yes=("is_yes", "any"),  # Whether there is any "Yes"
            how_many_national_stockouts=("is_yes", "sum"),  # Count of "Yes" (True is treated as 1)
        )
        .assign(any_national_vaccine_stockout=lambda df: df["any_yes"].map({True: "Yes", False: "No"}))
        .drop(columns=["any_yes"])
    )

    tb = tb.merge(tb_agg, on=["country", "year"], how="left")

    return tb


def district_level_stockout_for_any_vaccine(tb: Table) -> Table:
    """
    Was there a vaccine stockout at the district-level of any vaccine in the country in the given year?
    """
    # Get the rows relevant to district level vaccine stockouts (i.e. not national, or stockouts of syringes)

    tb_agg = tb[
        tb["description"].str.contains("Was there a vaccine stock-out at the district level")
        & (tb["description"].str.contains("vaccine"))
    ]
    tb_agg = tb_agg.dropna(subset=["value"])
    tb_agg = (
        tb_agg.assign(is_yes=tb_agg["value"].eq("Yes"))
        .groupby(["country", "year"], as_index=False)["is_yes"]
        .any()
        .assign(any_district_level_vaccine_stockout=lambda df: df["is_yes"].map({True: "Yes", False: "No"}))
        .drop(columns=["is_yes"])
    )

    tb = tb.merge(tb_agg, on=["country", "year"], how="left")

    return tb
