"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Dataset codes to select, and their corresponding names.
DATASET_CODES_AND_NAMES = {
    "nrg_pc_202": "Gas prices for household consumers",  # bi-annual data (from 2007)
    "nrg_pc_203": "Gas prices for non-household consumers",  # bi-annual data (from 2007)
    "nrg_pc_204": "Electricity prices for household consumers",  # bi-annual data (from 2007)
    "nrg_pc_205": "Electricity prices for non-household consumers",  # bi-annual data (from 2007)
    # "nrg_pc_202_v": "Gas consumption volumes for households", # annual data (from 2007)
    # "nrg_pc_203_v": "Gas consumption volumes for non-households", # annual data (from 2007)
    # "nrg_pc_204_v": "Electricity consumption volumes for households", # annual data (from 2007)
    # "nrg_pc_205_v": "Electricity consumption volumes for non-households", # annual data (from 2007)
    "nrg_pc_202_c": "Gas prices components for household consumers",  # annual data (from 2007)
    "nrg_pc_203_c": "Gas prices components for non-household consumers",  # annual data (from 2007)
    "nrg_pc_204_c": "Electricity prices components for household consumers",  # annual data (from 2007)
    "nrg_pc_205_c": "Electricity prices components for non-household consumers",  # annual data (from 2007)
    # "nrg_pc_206": "Share for transmission and distribution in the network cost for gas and electricity", # annual data (from 2007)
    "nrg_pc_202_h": "Gas prices for domestic consumers",  # bi-annual data (until 2007)
    "nrg_pc_203_h": "Gas prices for industrial consumers",  # bi-annual data (until 2007)
    "nrg_pc_204_h": "Electricity prices for domestic consumers",  # bi-annual data (until 2007)
    "nrg_pc_205_h": "Electricity prices for industrial consumers",  # bi-annual data (until 2007)
    "nrg_pc_206_h": "Electricity marker prices",  # bi-annual data (until 2007)
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gas_and_electricity_prices")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("gas_and_electricity_prices")

    #
    # Process data.
    #
    # Select relevant dataset codes, and add a column with the dataset name.
    tb = tb[tb["dataset_code"].isin(DATASET_CODES_AND_NAMES.keys())].reset_index(drop=True)
    tb["dataset_name"] = tb["dataset_code"].map(DATASET_CODES_AND_NAMES)

    # Sanity checks on inputs.
    # Ensure all relevant dataset codes are present.
    error = "Some dataset codes are missing."
    assert set(DATASET_CODES_AND_NAMES) <= set(tb["dataset_code"]), error
    # Check that each dataset has only one value in fields "freq", "product", and "nrg_cons".
    error = "Some datasets have more than one value in field 'freq'."
    assert (tb.groupby("dataset_code")["freq"].nunique() == 1).all(), error
    error = "Expected 'freq' column to be either A (annual) or S (bi-annual)."
    assert set(tb["freq"].dropna()) == set(["A", "S"]), error
    error = "Some datasets have more than one value in field 'product'."
    assert (tb.dropna(subset="product").groupby("dataset_code")["product"].nunique() == 1).all(), error
    error = "Expected 'product' column to be either 4100 (gas) or 6000 (electricity)."
    assert set(tb["product"].dropna()) == set([4100, 6000]), error

    # Drop unnecessary columns.
    tb = tb.drop(columns=["freq", "product"], errors="raise")

    # TODO: Continue here

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
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
