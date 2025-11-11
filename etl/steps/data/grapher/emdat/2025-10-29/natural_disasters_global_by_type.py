from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read table on yearly data.
    ds_garden = paths.load_dataset("natural_disasters")
    tb = ds_garden["natural_disasters_yearly"].reset_index()

    #
    # Process data.
    #
    # Select data for the World and remove unnecessary columns.
    tb_global = (
        tb[tb["country"] == "World"]
        .drop(columns=["country", "population", "gdp"], errors="raise")
        .reset_index(drop=True)
    )
    # Assign human-readable names to disaster types.
    tb_global["type"] = tb_global.astype({"type": str})["type"].replace(
        {disaster_type: disaster_type.capitalize().replace("_", " ") for disaster_type in tb_global["type"].unique()}
    )
    # Treat column for disaster type as the new entity (so they can be selected in grapher as if they were countries).
    tb_global = tb_global.rename(columns={"type": "country"}, errors="raise")

    # Set an appropriate index.
    tb_global = tb_global.format()

    tb_global.metadata.title = "Global natural disasters by type"
    tb_global.metadata.short_name = "natural_disasters_global_by_type"

    # Create new grapher dataset, update metadata, add table, and save dataset.
    ds_grapher = paths.create_dataset(tables=[tb_global])
    ds_grapher.save()
