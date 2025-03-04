"""Load a meadow dataset and create a garden dataset.

Adapted from Ed's code.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def define_orbits(tb: Table) -> Table:
    # Create a new column for the type of orbit.
    tb.loc[tb["periapsis"] <= 2000, "orbit"] = "Low Earth orbit"
    tb.loc[(tb["periapsis"] >= 2000) & (tb["periapsis"] <= 35586), "orbit"] = "Medium Earth orbit"
    tb.loc[(tb["periapsis"] >= 35586) & (tb["periapsis"] <= 35986), "orbit"] = "Geostationary orbit"
    tb.loc[tb["periapsis"] >= 35986, "orbit"] = "High Earth orbit"

    # Copy metadata from another column, and drop unnecessary columns.
    tb["orbit"] = tb["orbit"].copy_metadata(tb["periapsis"])
    tb = tb.drop(columns=["periapsis"])

    return tb


def create_year_columns(tb: Table) -> Table:
    # Create year columns for launch and decay dates, preserving metadata.
    for event in ["launch", "decay"]:
        tb[f"{event}_year"] = pr.to_datetime(tb[f"{event}_date"], format="%Y-%m-%d").dt.year
        tb[f"{event}_year"] = tb[f"{event}_year"].copy_metadata(tb[f"{event}_date"])

    return tb


def count_leo_by_type(tb: Table) -> Table:
    # Objects in Lower Earth orbit over time, broken down by object type

    tb = tb[tb.object_type.isin(["PAYLOAD", "ROCKET BODY", "DEBRIS"])]
    tb = tb[tb.orbit == "Low Earth orbit"]

    years = range(tb.launch_year.min().astype(int), tb.launch_year.max().astype(int) + 1)

    tables = []
    for year in years:
        # For each year, keep all launched objects up to that year & that haven't decayed yet
        tb_year = tb[(tb.launch_year <= year) & (tb.decay_date.isnull() | (tb.decay_year > year))]
        tb_year = (
            tb_year[["object_type"]].groupby("object_type", as_index=False, observed=True).size().assign(year=year)
        )
        tables.append(tb_year)

    leo_by_type = pr.concat(tables).reset_index(drop=True).rename(columns={"object_type": "entity", "size": "objects"})

    return leo_by_type


def count_non_debris_by_orbit(tb: Table) -> Table:
    # Non-debris objects in space over time, broken down by orbit

    tb = tb[tb.object_type.isin(["PAYLOAD", "ROCKET BODY"])]

    years = range(tb.launch_year.min().astype(int), tb.launch_year.max().astype(int) + 1)

    tables = []
    for year in years:
        # For each year, keep all launched objects up to that year & that haven't decayed yet
        tb_year = tb[(tb.launch_year <= year) & (tb.decay_date.isnull() | (tb.decay_year > year))]
        tb_year = tb_year[["orbit"]].groupby("orbit", as_index=False).size().assign(year=year)
        tables.append(tb_year)

    non_debris_by_orbit = (
        pr.concat(tables).reset_index(drop=True).rename(columns={"orbit": "entity", "size": "objects"})
    )  # type: ignore

    return non_debris_by_orbit


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("space_track")

    # Read table from meadow dataset.
    tb = ds_meadow.read("space_track", reset_index=False)

    #
    # Process data.
    #
    # Add a column with the orbit type.
    tb = define_orbits(tb=tb)

    # Create year columns for launch and decay.
    tb = create_year_columns(tb=tb)

    # Filter out data from the current year, which is incomplete.
    current_year = int(tb["launch_year"].metadata.origins[0].date_published[0:4])
    tb = tb[tb["launch_year"] < current_year].reset_index(drop=True)

    final = pr.concat([count_leo_by_type(tb), count_non_debris_by_orbit(tb)]).reset_index(drop=True)
    final["entity"] = final["entity"].replace(
        {"ROCKET BODY": "Rocket bodies", "PAYLOAD": "Payloads", "DEBRIS": "Debris"}
    )

    # Improve table format.
    final = final.format(["entity", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[final], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
