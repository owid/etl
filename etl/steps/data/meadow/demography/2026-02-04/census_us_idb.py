"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("census_us_idb.zip")

    # Load data from snapshot.
    with snap.extracted() as archive:
        tb = archive.read("idb5yr.txt", delimiter="|", force_extension="csv")

    #
    # Process data.
    #
    tb = tb.rename(
        columns={
            "#YR": "year",
        }
    )

    # Keep and process countries
    tb = filter_and_parse_country_codes(tb)

    # Improve tables format.
    tables = [
        tb.format(["country", "year"]),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(
        tables=tables,
        default_metadata=snap.metadata,
    )

    # Save meadow dataset.
    ds_meadow.save()


def filter_and_parse_country_codes(tb):
    """As per US Census site, codes starting with W14 correspond to countries
    - Go to https://www.census.gov/data/developers/data-sets/international-database.html
    - Click on "Geography" under "Time Series...": https://api.census.gov/data/timeseries/idb/5year/geography.html
    """
    # Keep only countries
    tb = tb[tb["GEO_ID"].str.startswith("W14")]
    # Sanity check 1: geo_id has the expected format
    # Verify geo_id format is 'W140000WOXX' where XX is the country code
    assert (tb["GEO_ID"].str.len().unique() == [11]).all()
    assert tb["GEO_ID"].str.match(r"^W140000WO[A-Z]{2}$").all(), "Unexpected geo_id format"

    # Extract country codes from geo_id (last 2 characters)
    tb.loc[:, "GEO_ID"] = tb["GEO_ID"].str[-2:]
    tb = tb.rename(columns={"GEO_ID": "country"})

    return tb
