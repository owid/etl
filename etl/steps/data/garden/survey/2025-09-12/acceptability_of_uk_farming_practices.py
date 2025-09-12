"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("acceptability_of_uk_farming_practices")

    # Read table from meadow dataset.
    tb = ds_meadow.read("acceptability_of_uk_farming_practices")

    #
    # Process data.
    #
    # TODO: Consider analysing demographics. For now, keep only total counts.

    # Rename columns.
    tb_counts = (
        tb.groupby(["question", "answer"], as_index=False)
        .agg({"user": "count"})
        .rename(columns={"user": ""})
        .pivot(index="question", columns="answer", join_column_levels_with="")
        .set_index("question")
    )
    # For some reason, indicators' metadata is not propagated, copy it from the original table.
    for column in tb_counts.columns:
        tb_counts[column] = tb_counts[column].copy_metadata(tb["answer"])
        tb_counts[column].metadata.unit = ""
        tb_counts[column].metadata.short_unit = ""

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_counts], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
