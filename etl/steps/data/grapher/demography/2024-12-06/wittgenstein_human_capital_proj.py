"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

AGE_ACCEPTED = [
    "total",
    "15+",
    "15-19",
    "20-39",
    "40-64",
    "65+",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("wittgenstein_human_capital_proj")

    # Read table from garden dataset.
    tables = {t.m.short_name: t for t in list(ds_garden)}

    # Filter out some dimensions, to make the step faster
    tb = tables["by_sex_age_edu"]
    index_cols = tb.index.names
    tb = tb.reset_index()
    tb = tb.loc[(tb["sex"] == "total") & (tb["age"].isin(AGE_ACCEPTED))]
    tables["by_sex_age_edu"] = tb.format(index_cols)

    #
    # Save outputs.
    #
    tables = list(tables.values())
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
