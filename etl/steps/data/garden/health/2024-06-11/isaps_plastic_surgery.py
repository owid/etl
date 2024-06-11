"""Load a meadow dataset and create a garden dataset."""

from owid.catalog.tables import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("isaps_plastic_surgery")

    # Read table from meadow dataset.
    tb = ds_meadow["isaps_plastic_surgery"].reset_index()

    # Rename columns
    tb = tb.rename(
        columns={
            "count": "num_procedures",
            "group_1": "category_1",
            "group_2": "category_2",
            "type": "procedure_name",
        }
    )

    # Groupbys
    tb_all_0 = tb.groupby(["country", "year"], as_index=False)[["num_procedures"]].sum()
    tb_all_1 = tb.groupby(["country", "year", "category_1"], as_index=False)[["num_procedures"]].sum()
    tb_all_2 = tb.groupby(["country", "year", "category_1", "category_2"], as_index=False)[["num_procedures"]].sum()
    tb = concat([tb, tb_all_0, tb_all_1, tb_all_2])

    columns = ["category_1", "category_2", "procedure_name"]
    tb[columns] = tb[columns].astype("string").fillna("all")

    #
    # Process data.
    #
    # tb = geo.harmonize_countries(
    #     df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    # )
    tb = tb.format(["country", "year", "category_1", "category_2", "procedure_name"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
