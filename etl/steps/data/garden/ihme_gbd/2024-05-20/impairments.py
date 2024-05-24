"""Load a meadow dataset and create a garden dataset."""

from typing import List

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("impairments")

    # Read table from meadow dataset.
    tb = ds_meadow["impairments"].reset_index()
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Dropping sex column as we only have values for both sexes
    if len(tb["sex"].unique() == 1):
        tb = tb.drop(columns="sex")
    # Split up the causes of blindness
    tb = other_vision_loss_minus_trachoma(tb)
    # Add region aggregates.
    tb = add_regional_aggregates(tb, ds_regions, index_cols=["country", "year", "metric", "cause", "impairment", "age"])

    cols = tb.columns.drop(["value"]).to_list()
    tb = tb.format(cols)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regional_aggregates(tb: Table, ds_regions: Dataset, index_cols: List[str]) -> Table:
    """
    Adding the regional aggregated data for the OWID continent regions
    """
    # Add population data
    tb = add_population(
        df=tb, country_col="country", year_col="year", age_col="age", age_group_mapping=AGE_GROUPS_RANGES
    )
    tb_number = tb[tb["metric"] == "Number"].copy()
    tb_rate = tb[tb["metric"] == "Rate"].copy()
    # Add region aggregates.
    tb_number = geo.add_regions_to_table(
        tb_number,
        index_columns=index_cols,
        regions=REGIONS,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    tb_rate_regions = tb_number[tb_number["country"].isin(REGIONS)].copy()
    tb_rate_regions["value"] = tb_number["value"] / tb_number["population"] * 100_000
    tb_rate_regions["metric"] = "Rate"

    tb_out = pr.concat([tb_number, tb_rate, tb_rate_regions], ignore_index=True)
    tb_out = tb_out.drop(columns=["population"])
    return tb_out


def other_vision_loss_minus_trachoma(tb: Table) -> Table:
    """
    To split up the causes of blindness we need to subtract trachoma from other vision loss
    """

    tb_other_vision_loss = tb[tb["cause"] == "Other vision loss"].copy()
    tb_trachoma = tb[tb["cause"] == "Trachoma"].copy()

    tb_combine = tb_other_vision_loss.merge(
        tb_trachoma, on=["country", "year", "metric", "impairment", "age"], suffixes=("", "_trachoma")
    )
    # Can I subtract rates if they have the same denominator? I think so
    tb_combine["value"] = tb_combine["value"] - tb_combine["value_trachoma"]
    tb_combine["cause"] = "Other vision loss minus trachoma"

    tb_combine = tb_combine.drop(columns=["value_trachoma", "cause_trachoma"])

    tb = pr.concat([tb, tb_combine], ignore_index=True)

    return tb
