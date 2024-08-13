"""Load a meadow dataset and create a garden dataset."""
import pandas as pd
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
    ds_meadow = paths.load_dataset("resettlement")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("resettlement")

    # filter out data before data availability starts (s. https://www.unhcr.org/refugee-statistics/methodology/, "Data publication timeline")
    tb["resettlement_arrivals"] = tb.apply(lambda x: x["resettlement_arrivals"] if x["year"] > 1958 else pd.NA, axis=1)
    tb["returned_refugees"] = tb.apply(lambda x: x["returned_refugees"] if x["year"] > 1964 else pd.NA, axis=1)
    tb["returned_idpss"] = tb.apply(lambda x: x["returned_idpss"] if x["year"] > 1996 else pd.NA, axis=1)
    tb["naturalisation"] = tb.apply(lambda x: x["naturalisation"] if x["year"] > 1989 else pd.NA, axis=1)

    # group table by country and year
    tb_origin = (
        tb.drop(columns=["country_of_asylum"])
        .groupby(["country_of_origin", "year"], observed=True)
        .sum(min_count=1)
        .reset_index()
    )
    tb_asylum = (
        tb.drop(columns=["country_of_origin"])
        .groupby(["country_of_asylum", "year"], observed=True)
        .sum(min_count=1)
        .reset_index()
    )

    # harmonize countries
    tb_origin = geo.harmonize_countries(
        df=tb_origin,
        country_col="country_of_origin",
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )
    tb_asylum = geo.harmonize_countries(
        df=tb_asylum,
        country_col="country_of_asylum",
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # merge tables
    tb = pr.merge(
        tb_origin,
        tb_asylum,
        left_on=["country_of_origin", "year"],
        right_on=["country_of_asylum", "year"],
        how="outer",
        suffixes=("_origin", "_dest"),
    )

    # merge country column (data is split between origin and asylum in columns)
    tb["country_of_origin"] = tb["country_of_origin"].fillna(tb["country_of_asylum"])
    tb = tb.rename(columns={"country_of_origin": "country"}, errors="raise")

    # drop country of asylum and idps destination (since it is identical to idps origin) column
    tb = tb.drop(columns=["country_of_asylum", "returned_idpss_dest"], errors="raise")

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
