"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("world_economic_outlook")

    # Read table from garden dataset.
    tb = ds_garden["world_economic_outlook"]

    #
    # Process data.
    #

    # For Grapher charts, we want the dashed projection line to start at the last observation so
    # that the line looks continuous. For this, we take each variable's last observation per country
    # and make it its first forecast as well.
    indicators = tb.columns.str.replace("_observation|_forecast", "", regex=True).unique().tolist()
    tb = tb.reset_index()

    for ind in indicators:
        # Find the last observation year by country
        last_obs = tb.loc[tb[ind + "_observation"].notnull()].groupby("country")["year"].max()
        # Assign that to last_obs column
        tb["last_obs"] = tb["country"].map(last_obs)
        # Where the year is the last_obs year, assign the value of the last observation
        tb.loc[tb["year"] == tb["last_obs"], ind + "_forecast"] = tb[ind + "_observation"]
        # Drop last_obs
        tb = tb.drop(columns="last_obs")

    # Reinstate the index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
