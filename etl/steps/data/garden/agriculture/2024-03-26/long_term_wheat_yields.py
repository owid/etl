"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Item code for "Wheat".
ITEM_CODE_FOR_WHEAT = "00000015"

# Element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load long-term wheat yield data from Bayliss-Smith & Wanmali (1984), and read its main table.
    ds_bayliss = paths.load_dataset("bayliss_smith_wanmali_1984")
    tb_bayliss = ds_bayliss["long_term_wheat_yields"].reset_index()

    # Load faostat data on crops and livestock products, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl["faostat_qcl"].reset_index()

    #
    # Process data.
    #
    # Select the relevant item and element from faostat data.
    # Also, select only countries that appear in the Bayliss-Smith & Wanmali (1984) dataset.
    tb_qcl = tb_qcl[
        (tb_qcl["item_code"] == ITEM_CODE_FOR_WHEAT)
        & (tb_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD)
        & (tb_qcl["country"].isin(tb_bayliss["country"].unique()))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units of yield have changed."
    assert list(tb_qcl["unit"].unique()) == ["tonnes per hectare"], error

    # Transpose data.
    tb_qcl = (
        tb_qcl.pivot(index=["country", "year"], columns="item", values="value")
        .reset_index()
        .rename(columns={"Wheat": "wheat_yield"}, errors="raise")
    )

    # Combine Bayliss and faostat data.
    combined = combine_two_overlapping_dataframes(df1=tb_qcl, df2=tb_bayliss, index_columns=["country", "year"])

    # Set an appropriate index and sort conveniently.
    combined = combined.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[combined], check_variables_metadata=True)
    ds_garden.save()
