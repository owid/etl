"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    tb = paths.read_snap_table("population_explore.xlsx")
    tb2 = paths.read_snap_table("population_explore_2025.xlsx")

    #
    # Process data.
    #
    tb = process_table(tb)
    tb2 = process_table(tb2)

    # Combine
    tb = tb.merge(tb2, on=["year", "country"], how="outer", suffixes=("", "_v2"))

    # Format
    tb = tb.format(["country", "year"], short_name="population_explore")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_table(tb):
    tb = tb.drop(index=range(0, 5))
    tb = tb.dropna(axis=1, how="all")

    tb = tb.melt(
        id_vars=["source"],
        var_name="country",
        value_name="population",
    ).rename(columns={"source": "year"})

    # Scale
    tb["population"] = (tb["population"].astype(float) * 1000).round().astype("Int64")

    return tb
