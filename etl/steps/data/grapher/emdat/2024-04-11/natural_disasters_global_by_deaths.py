from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read table on yearly data.
    ds_garden = paths.load_dataset("natural_disasters")
    tb = ds_garden["natural_disasters_yearly_deaths"].reset_index()

    #
    # Process data.
    #
    # Select data for the World and remove unnecessary columns.
    tb_global = tb[tb["country"] == "World"].drop(columns=["country"], errors="raise").reset_index(drop=True)

    # Drop also the column with unknown deaths.
    tb_global = tb_global.drop(columns=["n_events_with_unknown_deaths"], errors="raise")

    # Rename columns conveniently.
    tb_global = tb_global.rename(
        columns={
            column: column.replace("n_events_with_", "").replace("_", " ").capitalize()
            for column in tb_global.columns
            if column != "year"
        },
        errors="raise",
    )

    # Transform the table to long format, with a "country" column (called this way for compatibility with grapher).
    tb_global = tb_global.melt(id_vars=["year"], var_name="country", value_name="n_events")

    tb_global["n_events"].metadata.title = "Number of events"

    # Set an appropriate index.
    tb_global = tb_global.format()

    tb_global.metadata.title = "Global natural disasters by deaths"
    tb_global.metadata.short_name = "natural_disasters_global_by_deaths"

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_global], check_variables_metadata=True)
    ds_grapher.save()
