"""Load a garden dataset and create a grapher dataset."""

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

    tb_by_origin = tb.pivot(
        index=["country_origin", "year"],
        columns="country_destination",
        values=["migrants_all_sexes", "migrants_female", "migrants_male"],
    )

    tb_by_origin.columns = [col[0] + "_" + col[1] for col in tb_by_origin.columns]

    tb_by_dest = tb.pivot(
        index=["country_destination", "year"],
        columns="country_origin",
        values=["migrants_all_sexes", "migrants_female", "migrants_male"],
    )

    tb_by_dest.columns = [col[0] + "_" + col[1] for col in tb_by_dest.columns]

    # add metadata:

    for col in tb_by_origin.columns:
        cat = ""
        dest = ""
        if "migrants_all_sexes" in col:
            cat = "all"
            dest = col.split("migrants_all_sexes_")[1]
        elif "migrants_female" in col:
            cat = "female"
            dest = col.split("migrants_female_")[1]
        elif "migrants_male" in col:
            cat = "male"
            dest = col.split("migrants_male_")[1]
        tb_by_origin[col].metadata.unit = "people"
        tb_by_origin[col].metadata.short_unit = ""
        tb_by_origin[col].metadata.title = f"Number of {cat} emigrants who moved to {dest}"
        tb_by_origin[
            col
        ].metadata.description_short = f"Number of {cat} emigrants who have moved from the selected country to {dest}. The numbers describe cumulative migrant stock, not migrants who moved in this year."

    for col in tb_by_dest.columns:
        cat = ""
        origin = ""
        if "migrants_all_sexes" in col:
            cat = "all"
            origin = col.split("migrants_all_sexes_")[1]
        elif "migrants_female" in col:
            cat = "female"
            origin = col.split("migrants_female_")[1]
        elif "migrants_male" in col:
            cat = "male"
            origin = col.split("migrants_male_")[1]
        tb_by_dest[col].metadata.unit = "people"
        tb_by_dest[col].metadata.short_unit = ""
        tb_by_dest[col].metadata.title = f"Number of {cat} immigrants who moved from {origin}"
        tb_by_dest[
            col
        ].metadata.description_short = f"Number of {cat} immigrants who have moved to the selected country from {origin}. The numbers describe cumulative migrant stock, not migrants who moved in this year."

    tb_by_origin = tb_by_origin.reset_index()
    tb_by_origin = tb_by_origin.rename(columns={"country_origin": "country"})
    tb_by_origin.metadata.short_name = "migrant_stock_origin"
    tb_by_origin = tb_by_origin.format(["country", "year"])

    tb_by_dest = tb_by_dest.reset_index()
    tb_by_dest = tb_by_dest.rename(columns={"country_destination": "country"})
    tb_by_dest.metadata.short_name = "migrant_stock_destination"
    tb_by_dest = tb_by_dest.format(["country", "year"])

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_by_dest, tb_by_origin], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
