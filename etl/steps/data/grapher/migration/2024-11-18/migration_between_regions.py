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

    # Read table from garden dataset.
    tb = ds_garden["migration_between_regions"]

    tb = tb.pivot(columns="country_destination", index=["country_origin", "year"])

    tb.columns = [f"{col[1]}_{col[0]}" for col in tb.columns]

    # Add metadata.
    for col in tb.columns:
        tb[col].m.unit = "people"
        tb[col].m.short_unit = ""
        if "migrants_all_sexes" in col:
            dest = col.split("_migrants_all_sexes")[0]
            tb[col].m.description_short = f"Number of migrants to {dest.capitalize}"
        if "migrants_male" in col:
            dest = col.split("_migrants_male")[0]
            tb[col].m.description_short = f"Number of male migrants to {dest.capitalize}"
        if "migrants_female" in col:
            dest = col.split("_migrants_female")[0]
            tb[col].m.description_short = f"Number of female migrants to {dest.capitalize}"

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
