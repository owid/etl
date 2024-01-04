"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("all_indicators")
    # Read table from meadow dataset.
    tb = ds_meadow["all_indicators"].reset_index()

    # Load meadow dataset.
    ds_codes = paths.load_dataset("general_files")
    tb_codes = ds_codes["general_files"].reset_index()

    #
    # Process data.
    #
    # Add country name
    tb = tb.rename(columns={"country": "iso_code"}).astype({"iso_code": "str"})
    tb_codes = tb_codes.astype("str")
    tb = tb.merge(tb_codes, on="iso_code", how="left")
    tb.loc[tb["iso_code"] == "Total", "country"] = "World"
    # Drop columns
    tb = tb.drop(columns=["iso_code"])

    # Scale indicators
    ## Population indicators are given in 1,000
    columns_1000 = [
        "popc_c",
        "urbc_c",
        "rurc_c",
    ]
    tb[columns_1000] *= 1000
    ## Land use indicators are given in km2, but we want ha: 1km2 = 100ha
    columns_100 = [
        "uopp_c",
        "cropland_c",
        "tot_rice_c",
        "tot_rainfed_c",
        "rf_rice_c",
        "rf_norice_c",
        "tot_irri_c",
        "ir_rice_c",
        "ir_norice_c",
        "grazing_c",
        "pasture_c",
        "rangeland_c",
        "conv_rangeland_c",
        "shifting_c",
    ]
    tb[columns_100] *= 100

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    print(ds_garden["all_indicators"])
    # Save changes in the new garden dataset.
    ds_garden.save()
