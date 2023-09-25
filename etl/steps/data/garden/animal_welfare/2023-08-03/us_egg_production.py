"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_number_of_eggs(tb: Table) -> Table:
    # Select and rename columns.
    columns = {
        "observed_month": "month",
        "prod_type": "product",
        "prod_process": "housing",
        "n_hens": "number_of_hens",
        "n_eggs": "number_of_eggs",
    }
    tb = tb.loc[:, list(columns)].rename(columns=columns, errors="raise")

    # Add a column for year.
    tb["year"] = tb["month"].str[0:4]

    # Remove the day from the month column.
    tb["month"] = tb["month"].str[0:7]

    # Select only months for which we have all required data.
    # There should be 4 rows for each month: one row for hatching eggs, and three for table eggs (for "all", "cage-free (non-organic)" and "cage-free (organic)").
    # Therefore, keep only years that have 12 months, and 4 rows for each month (and skip first and last incomplete years).
    # tb = tb[tb.groupby("month", as_index=True)["product"].transform("count") == 4].reset_index(drop=True)
    tb = tb.loc[tb.groupby("year", as_index=True)["product"].transform("count") == 4 * 12, :].reset_index(drop=True)

    # Sanity checks.
    assert tb.groupby(["month"], as_index=False).count()["product"].unique().tolist() == [
        4
    ], "Expected 4 rows per month."
    assert tb.groupby(["year"], as_index=False).count()["product"].unique().tolist() == [
        12 * 4
    ], "Expected 12 months with 4 rows each per year."

    # Add data for hatching and table eggs for all months, to get the total number of hens and eggs per year.
    tb_total = (
        tb.drop(columns="month")
        .groupby(["year", "housing"])
        .agg({"number_of_hens": "sum", "number_of_eggs": "sum"})
        .reset_index()
    )
    tb_total = tb_total.copy_metadata(tb)

    # Reshape table.
    tb_total = tb_total.pivot(index=["year"], columns=["housing"], join_column_levels_with="_")

    # Ensure all columns are snake-case, and remove double underscores.
    tb_total = tb_total.underscore()
    tb_total = tb_total.rename(columns={column: column.replace("__", "_") for column in tb_total.columns})

    # Add a country column.
    tb_total["country"] = "United States"

    # Set an appropriate index and sort conveniently.
    tb_total = tb_total.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_total


def prepare_share_of_eggs(tb_share: Table) -> Table:
    # Select data for the last month of the year (which at least prior to 2016 seems to refer to yearly data).
    tb_share = tb_share[(tb_share["observed_month"].str[5:7] == "12")].reset_index(drop=True)

    # Add a year column.
    tb_share["year"] = tb_share["observed_month"].str[0:4].astype(int)
    # Some estimates come from USDA reports, whereas others are "computed".
    # For some years, there is data only from one of the two sources. And for some years, there is data from both.
    # The "computed" rows have data both for the percentage of hens and eggs, so we prioritize this over USDA reports.
    # In any case, the numbers from both sources are very similar.
    tb_share = (
        tb_share.sort_values(["year", "source"]).drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    )

    # Select necessary columns and rename them.
    columns = {"year": "year", "percent_hens": "share_of_hens_cage_free", "percent_eggs": "share_of_eggs_cage_free"}
    tb_share = tb_share[list(columns)].rename(columns=columns, errors="raise")

    # Add a country column.
    tb_share["country"] = "United States"

    # Set an appropriate index and sort conveniently.
    tb_share = tb_share.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb_share


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow: Dataset = paths.load_dependency("us_egg_production")
    tb = ds_meadow["us_egg_production"].reset_index()
    tb_share = ds_meadow["us_egg_production_share_cage_free"].reset_index()

    #
    # Process data.
    #
    # Prepare data for the number of eggs of different housing systems.
    tb = prepare_number_of_eggs(tb=tb)

    # Prepare the share of cage-free hens and eggs.
    tb_share = prepare_share_of_eggs(tb_share=tb_share)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_share], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
