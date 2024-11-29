"""Load a garden dataset and create a grapher dataset.
This grapher step has two purposes:
1. Format the data in a way that is compatible with the grapher database (split into two tables and index on country and year).
2. Add metadata programmatically to the data."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("migrant_stock")

    # Read table from garden dataset.
    tb = ds_garden.read("migrant_stock_dest_origin")

    tb = tb.drop(columns=["migrants_female", "migrants_male"])

    tb_dest_cols = tb.pivot(
        index=["country_origin", "year"],
        columns="country_destination",
        values=["migrants_all_sexes"],
    )

    tb_dest_cols.columns = [col[0] + "_to_" + col[1] for col in tb_dest_cols.columns]

    tb_origin_cols = tb.pivot(
        index=["country_destination", "year"],
        columns="country_origin",
        values=["migrants_all_sexes"],
    )

    tb_origin_cols.columns = [col[0] + "_from_" + col[1] for col in tb_origin_cols.columns]

    # add metadata:

    for col in tb_dest_cols.columns:
        dest = col.split("migrants_all_sexes_to_")[1]
        tb_dest_cols[col].metadata.unit = "people"
        tb_dest_cols[col].metadata.short_unit = ""
        tb_dest_cols[col].metadata.title = f"Number of immigrants who moved to {dest}"
        tb_dest_cols[
            col
        ].metadata.description_short = f"Number of migrants who have moved to {dest}. The numbers describe cumulative migrant stock, not migrants who moved in this year."

    for col in tb_origin_cols.columns:
        origin = col.split("migrants_all_sexes_from_")[1]

        tb_origin_cols[col].metadata.unit = "people"
        tb_origin_cols[col].metadata.short_unit = ""
        tb_origin_cols[col].metadata.title = f"Number of emigrants who moved from {origin}"
        tb_origin_cols[
            col
        ].metadata.description_short = f"Number of migrants who have moved to away from {origin}. The numbers describe cumulative migrant stock, not migrants who moved in this year."

    tb_dest_cols = tb_dest_cols.reset_index()
    tb_dest_cols = tb_dest_cols.rename(columns={"country_origin": "country"})
    tb_dest_cols.metadata.short_name = "migrant_stock_origin"
    tb_dest_cols = tb_dest_cols.format(["country", "year"])

    tb_origin_cols = tb_origin_cols.reset_index()
    tb_origin_cols = tb_origin_cols.rename(columns={"country_destination": "country"})
    tb_origin_cols.metadata.short_name = "migrant_stock_destination"
    tb_origin_cols = tb_origin_cols.format(["country", "year"])

    # Save outputs
    #
    # Create a new grapher dataset with the same metadata as the garden dataset
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_origin_cols, tb_dest_cols],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
