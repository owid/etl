"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def check_inputs(tb):
    # Gravitational wave events are named based on the date (and time, for more recent events) of their detection,
    # using the following format: "GW" + YYMMDD (date) + hhmmss (UTC time) + version.
    # Version is used if there are multiple versions of the data for the event (e.g., parameter estimation was improved).
    error = "A confident event appears multiple times with different versions."
    assert len(set(tb["id"].str.split("-").str[0])) == len(set(tb["id"])), error
    error = "Gravitational wave id format has changed."
    assert (tb["id"].str[0:2] == "GW").all(), error
    assert tb["id"].str[2:4].astype(int).max() <= int(tb["id"].metadata.origins[0].date_published[2:4]), error
    assert tb["id"].str[4:6].astype(int).max() <= 12, error
    assert tb["id"].str[6:8].astype(int).max() <= 31, error
    assert tb["id"].str[8].isin(["-", "_"]).all(), error
    assert (tb[tb["id"].str[8] == "-"]["id"].str[9] == "v").all(), error
    assert tb[tb["id"].str[8] == "_"]["id"].str[9:11].astype(int).max() <= 23, error
    assert tb[tb["id"].str[8] == "_"]["id"].str[11:13].astype(int).max() <= 59, error
    assert tb[tb["id"].str[8] == "_"]["id"].str[13:15].astype(int).max() <= 59, error
    assert (tb[tb["id"].str[8] == "_"]["id"].str[16] == "v").all(), error


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gravitational_wave_events")

    # Read table from meadow dataset.
    tb = ds_meadow.read("gravitational_wave_events")

    #
    # Process data.
    #
    # Select only confident detections.
    tb = tb[(tb["catalog_shortname"].str.contains("confident"))].reset_index(drop=True)

    # Sanity check inputs.
    check_inputs(tb=tb)

    # Extract discovery year from the event name.
    tb["year"] = "20" + tb["id"].str[2:4]

    # Create a table with the number of detections per year.
    tb = tb.groupby("year", as_index=False).agg({"id": "size"}).rename(columns={"id": "n_events"})

    # Add a column with the cumulative number of detections.
    tb = tb.sort_values("year").reset_index(drop=True)
    tb["n_events_cumulative"] = tb["n_events"].cumsum()

    # Improve table format.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
