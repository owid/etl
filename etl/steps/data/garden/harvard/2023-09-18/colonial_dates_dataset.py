"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("colonial_dates_dataset")

    # Read table from meadow dataset.
    tb = ds_meadow["colonial_dates_dataset"].reset_index()

    # Capitalize colonizer column and replace Britain by United Kingdom
    tb["colonizer"] = tb["colonizer"].str.capitalize().replace("Britain", "United Kingdom")

    # Create a year column with one value per row representing the range between colstart_max and colend_max
    tb["year"] = tb.apply(lambda x: list(range(x["colstart_max"], x["colend_max"] + 1)), axis=1)

    # Explode the year column
    tb = tb.explode("year").reset_index(drop=True)

    # Drop colstart and colend columns
    tb = tb.drop(columns=["colstart_max", "colend_max", "colstart_mean", "colend_mean"])

    # Create another table with the total number of colonies per colonizer and year
    tb_count = tb.groupby(["colonizer", "year"]).agg({"country": "count"}).reset_index().copy_metadata(tb)

    # Rename columns
    tb_count = tb_count.rename(columns={"colonizer": "country", "country": "total_colonies"})

    # Consolidate results in country and year columns, by merging colonizer column in each row
    tb = tb.groupby(["country", "year"]).agg({"colonizer": lambda x: " - ".join(x)}).reset_index().copy_metadata(tb)

    # Create an additional summarized colonizer column, replacing the values with " - " with "More than one colonizer"
    # I add the "z." to have this at the last position of the map brackets
    tb["colonizer_grouped"] = tb["colonizer"].apply(
        lambda x: "z. Multiple colonizers" if isinstance(x, str) and " - " in x else x
    )

    # Copy table to not lose metadata
    tb2 = tb.copy()

    # Create last_colonizer column, which is the most recent non-null colonizer for each country and year
    tb2["last_colonizer"] = tb2.groupby(["country"])["colonizer"].fillna(method="ffill")
    tb2["last_colonizer_grouped"] = tb2.groupby(["country"])["colonizer_grouped"].fillna(method="ffill")

    tb2 = tb2.copy_metadata(tb)
    for col in ["last_colonizer", "last_colonizer_grouped"]:
        tb2[col].metadata = tb.colonizer.metadata

    # Merge both tables
    tb = pr.merge(tb2, tb_count, on=["country", "year"], how="left", short_name="colonial_dates_dataset")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
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
