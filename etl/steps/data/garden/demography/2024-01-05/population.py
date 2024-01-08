"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load UN WPP dataset.
    ds_un = paths.load_dataset("un_wpp")
    tb_un = ds_un["un_wpp"].reset_index()
    ds_hyde = paths.load_dataset("all_indicators")
    tb_hyde = ds_hyde["all_indicators"].reset_index()

    #
    # Process data.
    #
    columns_index = ["country", "year"]
    # Format hyde
    tb_hyde = tb_hyde[columns_index + ["popc_c"]]
    # Format wpp
    tb_un = tb_un.loc[
        (tb_un["metric"] == "population") & (tb_un["sex"] == "all") & (tb_un["age"] == "all"),
        ["location", "year", "variant", "value"],
    ]
    tb_un_estimates = tb_un[tb_un["variant"] == "estimates"].drop(columns=["variant"])
    tb_un_projections = tb_un[tb_un["variant"] == "medium"].drop(columns=["variant"])
    column_rename = {
        "location": "country",
        "value": "population",
    }
    tb_un_estimates = tb_un_estimates.rename(columns=column_rename, errors="ignore")
    tb_un_projections = tb_un_projections.rename(columns=column_rename, errors="ignore")

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
