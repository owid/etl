"""Load historical data on UK yields and combine it with the latest FAOSTAT data."""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"

# Item codes for required items.
ITEM_CODE_FOR_WHEAT = "00000015"
ITEM_CODE_FOR_BARLEY = "00000044"
ITEM_CODE_FOR_OATS = "00000075"
ITEM_CODE_FOR_POTATOES = "00000116"
ITEM_CODE_FOR_PULSES = "00001726"
ITEM_CODE_FOR_RYE = "00000071"
ITEM_CODE_FOR_SUGAR_BEET = "00000157"
ITEM_CODES = [
    ITEM_CODE_FOR_WHEAT,
    ITEM_CODE_FOR_BARLEY,
    ITEM_CODE_FOR_OATS,
    ITEM_CODE_FOR_POTATOES,
    ITEM_CODE_FOR_PULSES,
    ITEM_CODE_FOR_RYE,
    ITEM_CODE_FOR_SUGAR_BEET,
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load UK long-term yields data from Broadberry et al. (2015), and read its main table.
    ds_broadberry = paths.load_dataset("broadberry_et_al_2015")
    tb_broadberry = ds_broadberry["broadberry_et_al_2015"].reset_index()

    # Load UK long-term yields data from Brassley (2000), and read its main table.
    ds_brassley = paths.load_dataset("brassley_2000")
    tb_brassley = ds_brassley["brassley_2000"].reset_index()

    # Load faostat data on crop and livestock production, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl["faostat_qcl"].reset_index()

    #
    # Process data.
    #
    # Select required country, element and items.
    tb_qcl = tb_qcl[
        (tb_qcl["country"] == "United Kingdom")
        & (tb_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD)
        & (tb_qcl["item_code"].isin(ITEM_CODES))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units for yield have changed."
    assert list(tb_qcl["unit"].unique()) == ["tonnes per hectare"], error

    # Transpose data.
    tb_qcl = tb_qcl.pivot(index=["country", "year"], columns=["item"], values=["value"], join_column_levels_with="_")
    tb_qcl = tb_qcl.rename(
        columns={
            column: column.lower().replace("value_", "").replace(" ", "_") + "_yield"
            for column in tb_qcl.columns
            if column not in ["country", "year"]
        },
        errors="raise",
    )

    # Combine historical data.
    tb_historical = combine_two_overlapping_dataframes(
        df1=tb_broadberry, df2=tb_brassley, index_columns=["country", "year"]
    )

    # Combine historical data with faostat data.
    tb_garden = combine_two_overlapping_dataframes(df1=tb_qcl, df2=tb_historical, index_columns=["country", "year"])

    # Set an appropriate index and sort conveniently.
    tb_garden = tb_garden.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_garden.save()
