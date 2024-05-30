"""Load a meadow dataset and create a garden dataset."""


from owid.catalog import Table
from owid.catalog import processing as pr
from shared import add_regional_aggregates

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
    tb = other_vision_loss_minus_some_causes(tb)
    # Add region aggregates.
    tb = add_regional_aggregates(
        tb,
        ds_regions,
        index_cols=["country", "year", "metric", "cause", "impairment", "age"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
    )

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


def other_vision_loss_minus_some_causes(tb: Table) -> Table:
    """
    To split up the causes of blindness we need to subtract trachoma, onchocerciasis and malaria from other vision loss
    """
    causes_to_subtract = ["Trachoma", "Onchocerciasis", "Malaria"]

    tb_other_vision_loss = tb[
        (tb["cause"] == "Other vision loss") & (tb["metric"] == "Number") & (tb["impairment"] == "Blindness")
    ].copy()
    # Get the trachoma, malaria and onchocerciasis data
    tb[["cause", "metric", "impairment"]] = tb[["cause", "metric", "impairment"]].astype(str)
    msk = (tb["cause"].isin(causes_to_subtract)) & (tb["metric"] == "Number") & (tb["impairment"] == "Blindness")
    tb_trachoma = tb[msk].copy()
    tb_trachoma = tb_trachoma.groupby(["country", "year", "metric", "impairment", "age"])["value"].sum().reset_index()
    tb_trachoma["cause"] = "Trachoma, malaria and onchocerciasis"

    tb_combine = tb_other_vision_loss.merge(
        tb_trachoma, on=["country", "year", "metric", "impairment", "age"], suffixes=("", "_trachoma")
    )
    tb_combine["value"] = tb_combine["value"] - tb_combine["value_trachoma"]
    tb_combine["cause"] = "Other vision loss minus trachoma, malaria and onchocerciasis"

    tb_combine = tb_combine.drop(columns=["value_trachoma", "cause_trachoma"])

    tb = pr.concat([tb, tb_combine], ignore_index=True)

    return tb
