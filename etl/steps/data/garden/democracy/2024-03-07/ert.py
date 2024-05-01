"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ert")

    # Read table from meadow dataset.
    tb = ds_meadow["ert"].reset_index()

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )

    # Clean regime_dich_ert
    tb["regime_dich_ert"] = (
        tb["reg_type"]
        .astype("string")
        .replace(
            {
                "0": "autocracy",
                "1": "democracy",
            }
        )
    )
    tb = tb.drop(columns=["reg_type"])

    # Add regime_ert
    column = "regime_ert"
    assert set(tb["dem_ep"]) == {0, 1}, "`dem_ep` must only contain values {0,1}"
    assert set(tb["aut_ep"]) == {0, 1}, "`aut_ep` must only contain values {0,1}"
    tb.loc[
        (tb["regime_dich_ert"] == "autocracy") & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 0), column
    ] = "hardening autocracy"
    tb.loc[
        (tb["regime_dich_ert"] == "autocracy") & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 0), column
    ] = "stable autocracy"
    tb.loc[
        (tb["regime_dich_ert"] == "autocracy") & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 1), column
    ] = "liberalizing autocracy"
    tb.loc[
        (tb["regime_dich_ert"] == "democracy") & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 0), column
    ] = "eroding democracy"
    tb.loc[
        (tb["regime_dich_ert"] == "democracy") & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 0), column
    ] = "stable democracy"
    tb.loc[
        (tb["regime_dich_ert"] == "democracy") & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 1), column
    ] = "deepening democracy"
    tb.loc[(tb["regime_dich_ert"] == "autocracy") & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 1), column] = float("nan")
    tb.loc[(tb["regime_dich_ert"] == "democracy") & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 1), column] = float("nan")
    tb[column] = tb[column].copy_metadata(tb["regime_dich_ert"])

    # Add regime_trich_ert
    column = "regime_trich_ert"
    tb.loc[(tb["regime_ert"] == 0) | (tb["regime_ert"] == 3), column] = "autocratizing regime"
    tb.loc[(tb["regime_ert"] == 1) | (tb["regime_ert"] == 4), column] = "stable regime"
    tb.loc[(tb["regime_ert"] == 2) | (tb["regime_ert"] == 5), column] = "democratizing regime"
    tb[column] = tb[column].copy_metadata(tb["regime_dich_ert"])

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
