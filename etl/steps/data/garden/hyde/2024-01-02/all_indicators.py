"""Load a meadow dataset and create a garden dataset."""
from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Run step."""
    #
    # Load inputs.
    #
    # Load 'meadow dataset.'
    ds_meadow = paths.load_dataset("all_indicators")

    # Read 'all_indicators' table, which contains the actual indicators
    tb = ds_meadow["all_indicators"].reset_index()

    # Read 'general_files' table, which contains the country codes (used to map country codes to country names)
    tb_codes = ds_meadow["general_files"].reset_index()

    #
    # Process data.
    #
    # Standardise country codes in codes table
    tb_codes = geo.harmonize_countries(
        df=tb_codes,
        countries_file=paths.country_mapping_path,
    )

    # Add country name to main table
    tb = tb.rename(columns={"country": "iso_code"}, errors="raise").astype({"iso_code": "str"})
    tb_codes = tb_codes.astype("str")
    tb = tb.merge(tb_codes, on="iso_code", how="left")
    tb.loc[tb["iso_code"] == "Total", "country"] = "World"
    # Drop columns
    tb = tb.drop(columns=["iso_code"], errors="raise")

    # Scale indicators
    ## Population indicators are given in 1,000
    tb[
        [
            "popc_c",
            "urbc_c",
            "rurc_c",
        ]
    ] *= 1000
    ## Land use indicators are given in km2, but we want ha: 1km2 = 100ha
    tb[
        [
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
    ] *= 100

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

    # Save changes in the new garden dataset.
    ds_garden.save()
