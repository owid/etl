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
    ds_meadow = paths.load_dataset("undp_hdr")

    # Read table from meadow dataset.
    tb = ds_meadow["undp_hdr"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Drop irrelevant columns
    tb = tb.drop(columns=["iso3", "hdicode", "region"])

    # Re-shape table to get (country, year) as index and variables as columns.
    tb = tb.melt(id_vars=["country"])
    tb[["variable", "year"]] = tb["variable"].str.extract(r"(.*)_(\d{4})")
    tb = tb.pivot(index=["country", "year"], columns="variable", values="value").reset_index()

    # Make Atkinson indices not percentages
    atkinson_cols = ["ineq_edu", "ineq_inc", "ineq_le", "coef_ineq"]
    for col in atkinson_cols:
        tb[col] /= 100

    # Set dtypes
    tb = tb.astype(
        {
            "country": "category",
            "year": int,
            **{col: "Float64" for col in tb.columns if col not in ["country", "year"]},
        }
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
