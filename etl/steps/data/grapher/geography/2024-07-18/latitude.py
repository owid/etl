"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("nat_earth_110")

    # Read table from garden dataset.
    tb = ds_garden["nat_earth_110"].reset_index()
    tb = tb[["name", "latitude_centroid", "absolute_latitude"]]
    # tb["year"] = 2024
    tb = tb.rename(columns={"name": "country"})
    # Remove entities that are not in the DB - they are needed for another use of this dataset, so I didn't want to remove them in garden
    entities_to_remove = [
        "Southern Patagonian Ice Field",
        "Serranilla Bank",
        "Akrotiri",
        "Fr. S. Antarctic Lands",
        "S. Geo. and the Is.",
        "Clipperton I.",
        "Coral Sea Is.",
        "USNB Guantanamo Bay",
        "U.S. Minor Outlying Is.",
        "Cyprus U.N. Buffer Zone",
        "Scarborough Reef",
        "Dhekelia",
        "Brazilian I.",
        "Baikonur",
        "Ashmore and Cartier Is.",
        "Bir Tawil",
        "Indian Ocean Ter.",
        "Bajo Nuevo Bank",
        "Siachen Glacier",
        "Spratly Is.",
    ]
    tb = tb[~tb["country"].isin(entities_to_remove)]
    # Process data.
    tb = add_each_year(tb)
    #
    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def add_each_year(tb: Table) -> Table:
    """
    Duplicate the data for each year between 1900 and 2100.
    """
    years = range(1900, 2101)
    tables = []
    for year in years:
        tb_copy = tb.copy()  # Create a copy of the DataFrame to avoid modifying the original
        tb_copy["year"] = year  # Add the year as a new column
        tables.append(tb_copy)
    tb = pr.concat(tables)
    return tables
