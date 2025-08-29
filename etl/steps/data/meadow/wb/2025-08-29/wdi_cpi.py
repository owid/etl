"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load WDI auxiliary snapshot for Consumer Price Index.
    snap = paths.load_snapshot("wdi_cpi.zip")
    tb = snap.read_in_archive(filename="API_FP.CPI.TOTL_DS2_en_csv_v2_507900.csv", skiprows=4)

    #
    # Process data.
    #
    # Sanity check.
    error = "Downloaded snapshot file has changed format."
    assert set(tb["Indicator Code"]) == {"FP.CPI.TOTL"}, error
    assert set(tb["Indicator Name"]) == {"Consumer price index (2010 = 100)"}, error

    # Drop unnecessary columns and rename columns conveniently.
    tb = tb.drop(columns=["Country Code", "Indicator Name", "Indicator Code"], errors="raise").rename(
        columns={"Country Name": "country"}, errors="raise"
    )

    # Transpose table to have a year column.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="cpi")

    # Drop rows with no data.
    tb = tb.dropna(axis=0, subset="cpi").reset_index(drop=True)

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_meadow.save()
