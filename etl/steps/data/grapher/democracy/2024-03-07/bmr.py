"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("bmr")

    # Read table from garden dataset.
    tb = ds_garden["bmr"]

    #
    # Process data.
    #
    # Special indicator values renamings
    cols = [
        # "num_years_in_democracy",
        # "num_years_in_democracy_ws",
        "num_years_in_democracy_consecutive",
        "num_years_in_democracy_ws_consecutive",
    ]
    for col in cols:
        tb[col] = tb[col].astype("string").replace({"0": "non-democracy"})

    # Drop indicators (only useful in Garden)
    columns_drop = [
        "regime_imputed_country",
        "regime_imputed",
        "num_years_in_democracy_consecutive_group",
        "num_years_in_democracy_ws_consecutive_group",
    ]
    tb = tb.drop(columns=columns_drop)

    #
    # Save outputs.
    #
    tables = [
        tb,
        ds_garden["num_countries_regime"],
        ds_garden["num_countries_regime_years"],
        ds_garden["population_regime"],
        ds_garden["population_regime_years"],
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
