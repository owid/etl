"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("refugee_data")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("refugee_data")

    # group table by country_of_origin and year
    tb_origin = tb.drop(columns=["country_of_asylum"]).groupby(["country_of_origin", "year"]).sum().reset_index()
    tb_asylum = tb.drop(columns=["country_of_origin"]).groupby(["country_of_asylum", "year"]).sum().reset_index()

    tb_origin = geo.harmonize_countries(
        df=tb_origin, country_col="country_of_origin", countries_file=paths.country_mapping_path
    )
    tb_asylum = geo.harmonize_countries(
        df=tb_asylum, country_col="country_of_asylum", countries_file=paths.country_mapping_path
    )

    # merge tables
    tb = pr.merge(
        tb_origin,
        tb_asylum,
        left_on=["country_of_origin", "year"],
        right_on=["country_of_asylum", "year"],
        how="outer",
        suffixes=("_origin", "_asylum"),
    )

    tb["country_of_origin"] = tb["country_of_origin"].fillna(tb["country_of_asylum"])

    # drop country of asylum column
    tb = tb.rename(columns={"country_of_origin": "country"}).drop(columns=["country_of_asylum"])

    # format table
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
