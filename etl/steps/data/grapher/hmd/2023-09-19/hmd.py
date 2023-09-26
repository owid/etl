"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("hmd")

    # Read table from garden dataset.
    tb = ds_garden["hmd"]

    #
    # Process data.
    #
    ## Reset index
    column_index = list(tb.index.names)
    tb = tb.reset_index()

    ## Only keep 5-year age groups, and 1-year observation periods
    tb_5 = tb[tb["format"] == "5x1"]
    ## TODO: Exceptionally add single-year age groups (maybe 0, 10, 15, etc.)
    tb_1 = tb[tb["format"] == "1x1"]
    ages_single = [0, 10, 15, 25, 45, 65, 80]
    ages_single = list(map(str, ages_single))
    tb_1 = tb_1[tb_1["age"].isin(ages_single)]

    ## Combine
    tb = pr.concat([tb_5, tb_1], ignore_index=True)

    ## Set dtype of year to int
    tb["year"] = tb["year"].astype("Int64")

    ## Set index back
    tb = tb.set_index(column_index, verify_integrity=True).sort_index()

    ## Only keep subset of columns
    tb = tb[["central_death_rate", "life_expectancy", "probability_of_death"]]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
