"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

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

    # Create two tables, one for colonized countries and one for colonizers/not colonized countries
    tb_colonized = tb[tb["col"] == "1"].reset_index(drop=True)
    tb_rest = tb[tb["col"] == "0"].reset_index(drop=True)

    # Generate a list of the colonizers
    colonizers_list = tb_rest["colonizer"].unique().tolist()

    # Remove duplicates for tb_rest
    tb_rest = tb_rest.drop_duplicates(subset=["country"], keep="first").reset_index(drop=True)

    # Assign the value "Not colonized" to colonizer column
    # tb_rest["colonizer"] = "Not colonized"

    # For countries in colonizers_list, assign the value "Colonizer" to colonizer column
    tb_rest["colonizer"] = tb_rest["colonizer"].where(~tb_rest["country"].isin(colonizers_list), "Colonizer")

    # For these countries, assign the minimum year of colstart_max as colstart_max and the maximum year of colend_max as colend_max
    tb_rest["colstart_max"] = tb_colonized["colstart_max"].min()
    tb_rest["colend_max"] = tb_colonized["colend_max"].max()

    tb = pr.concat([tb_colonized, tb_rest], ignore_index=True)

    # Create a year column with one value per row representing the range between colstart_max and colend_max
    # NOTE: I have decided to use last date aggregations, but we could also use mean aggregations
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
