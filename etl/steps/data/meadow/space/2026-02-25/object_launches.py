"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select, and how to rename them.
COLUMNS = {
    "object.launch.stateOfRegistry_s1": "country",
    "object.launch.dateOfLaunch_s1": "year",
    "object.nameOfSpaceObjectO_s1": "name",
    "object.nameOfSpaceObjectIno_s1": "name_ino",
}


def plot_starlink_objects(tb):
    # Just out of curiosity, plot how many of all objects are Starlink satellites.
    import plotly.express as px

    tb_plot = tb.copy()
    tb_plot["is_starlink"] = tb_plot["name"].str.lower().str.contains("starlink", na=False)
    tb_plot = tb_plot.groupby(["year"], as_index=False).agg(
        total_objects=("name", "size"), starlink_objects=("is_starlink", "sum")
    )
    px.line(tb_plot.melt(id_vars=["year"]), x="year", y="value", color="variable", markers=True).show()


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("object_launches.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # UNOOSA uses two different name fields for different time periods:
    # - object.nameOfSpaceObjectO_s1: Used for older objects (pre-2025 mostly)
    # - object.nameOfSpaceObjectIno_s1: Used for newer objects (2025 onwards)
    # Combine both fields to get complete name coverage.
    tb["name"] = tb["name"].fillna(tb["name_ino"])
    tb = tb.drop(columns=["name_ino"])

    # Create a year column.
    tb["year"] = tb["year"].str[0:4].astype(int)

    # Uncomment to plot the total number of objects and the number of Starlink objects.
    # plot_starlink_objects(tb=tb)

    # Add the number of launches for each country and year (and add metadata to the new column).
    tb = tb.groupby(["country", "year"], as_index=False).size().rename(columns={"size": "annual_launches"})
    tb["annual_launches"] = tb["annual_launches"].copy_metadata(tb["country"])

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
