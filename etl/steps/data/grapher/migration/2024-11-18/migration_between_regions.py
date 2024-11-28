"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("migration_between_regions")

    # Read table from garden dataset
    tb = ds_garden["migration_between_regions"].reset_index()

    tb = tb.pivot(columns="country_destination", index=["country_origin", "year"], values="migrants_all_sexes")

    # Add metadata.
    for col in tb.columns:
        tb[col].m.unit = "people"
        tb[col].m.short_unit = ""
        tb[col].m.title = f"Migration to {col}"
        tb[col].m.description_short = f"Number of migrants to {col}"

    tb = tb.reset_index()

    tb["country"] = tb["country_origin"]

    tb = tb.drop(columns=["country_origin"]).format(["country", "year"])

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
