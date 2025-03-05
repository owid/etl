"""Load a meadow dataset and create a garden dataset.

Adapted from Ed's code.
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected object types in the data, and how to rename them.
OBJECT_TYPES = {
    "ROCKET BODY": "Rocket bodies",
    "PAYLOAD": "Payloads",
    "DEBRIS": "Debris",
}


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


def count_lower_earth_orbit_objects(tb: Table) -> Table:
    # Select objects in Lower Earth orbit over time, broken down by object type.
    tb = tb[
        tb["object_type"].isin(["PAYLOAD", "ROCKET BODY", "DEBRIS"]) & (tb["orbit"] == "Low Earth orbit")
    ].reset_index(drop=True)

    # Create a list of all years, from the first to the last informed in the data.
    years = range(int(tb["launch_year"].min()), int(tb["launch_year"].max()) + 1)

    # For each year, keep all launched objects up to that year that haven't decayed yet.
    tables = []
    for year in years:
        tb_year = tb[(tb["launch_year"] <= year) & (tb["decay_date"].isnull() | (tb["decay_year"] > year))]
        tb_year = (
            tb_year[["object_type"]]
            .groupby("object_type", as_index=True, observed=True)
            .agg({"object_type": "size"})
            .rename(columns={"object_type": "n_objects"})
            .reset_index()
            .assign(**{"year": year})
        )
        tables.append(tb_year)

    # Combine all tables.
    tb_lower_earth_objects_by_type = pr.concat(tables).reset_index(drop=True)

    # Rename object types for convenience.
    tb_lower_earth_objects_by_type["object_type"] = map_series(
        tb_lower_earth_objects_by_type["object_type"],
        mapping=OBJECT_TYPES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )

    return tb_lower_earth_objects_by_type


def count_non_debris_objects(tb: Table) -> Table:
    # Select non-debris objects in space over time, broken down by orbit.
    tb = tb[tb["object_type"].isin(["PAYLOAD", "ROCKET BODY"])].reset_index(drop=True)

    # Create a list of all years, from the first to the last informed in the data.
    years = range(int(tb["launch_year"].min()), int(tb["launch_year"].max()) + 1)

    # For each year, keep all launched objects up to that year & that haven't decayed yet.
    tables = []
    for year in years:
        tb_year = tb[(tb["launch_year"] <= year) & (tb["decay_date"].isnull() | (tb["decay_year"] > year))]
        tb_year = (
            tb_year[["orbit"]]
            .groupby("orbit", as_index=True, observed=True)
            .agg({"orbit": "size"})
            .rename(columns={"orbit": "n_objects"})
            .reset_index()
            .assign(**{"year": year})
        )
        tables.append(tb_year)

    # Combine all tables.
    tb_non_debris_objects_by_orbit = pr.concat(tables).reset_index(drop=True)

    return tb_non_debris_objects_by_orbit


def run() -> None:
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

    # Create a table with the number of objects in Lower Earth orbit by type.
    tb_lower_earth_objects_by_type = count_lower_earth_orbit_objects(tb=tb)

    # Create a table with the number of non-debris objects in space by orbit.
    tb_non_debris_objects_by_orbit = count_non_debris_objects(tb=tb)

    # Improve table formats.
    tb_lower_earth_objects_by_type = tb_lower_earth_objects_by_type.format(
        ["object_type", "year"], short_name="lower_earth_objects_by_type"
    )
    tb_non_debris_objects_by_orbit = tb_non_debris_objects_by_orbit.format(
        ["orbit", "year"], short_name="non_debris_objects_by_orbit"
    )

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_lower_earth_objects_by_type, tb_non_debris_objects_by_orbit],
        default_metadata=ds_meadow.metadata,
    )

    # Save garden dataset.
    ds_garden.save()
