"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def _load_wdi_snapshot(snap):
    with snap.open_archive():
        file_name = [file.name for file in list(snap._unarchived_dir.glob("*")) if "Metadata" not in file.name][0]
        tb = snap.read_from_archive(filename=file_name, skiprows=4)

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load WDI auxiliary snapshots.
    snap_cpi = paths.load_snapshot("wdi_cpi.zip")
    snap_gdp = paths.load_snapshot("wdi_gdp.zip")
    tb_cpi = _load_wdi_snapshot(snap=snap_cpi)
    tb_gdp = _load_wdi_snapshot(snap=snap_gdp)

    #
    # Process data.
    #
    # Sanity check.
    error = "Downloaded snapshot file has changed format."
    assert set(tb_cpi["Indicator Code"]) == {"FP.CPI.TOTL"}, error
    assert set(tb_cpi["Indicator Name"]) == {"Consumer price index (2010 = 100)"}, error
    assert set(tb_gdp["Indicator Code"]) == {"NY.GDP.DEFL.ZS"}, error
    assert set(tb_gdp["Indicator Name"]) == {"GDP deflator (base year varies by country)"}, error

    # Drop unnecessary columns and rename columns conveniently.
    tb_cpi = (
        tb_cpi.drop(columns=["Country Code", "Indicator Name", "Indicator Code"], errors="raise")
        .rename(columns={"Country Name": "country"}, errors="raise")
        .melt(id_vars=["country"], var_name="year", value_name="cpi")
    )
    tb_gdp = (
        tb_gdp.drop(columns=["Country Code", "Indicator Name", "Indicator Code"], errors="raise")
        .rename(columns={"Country Name": "country"}, errors="raise")
        .melt(id_vars=["country"], var_name="year", value_name="gdp")
    )

    # Combine both tables.
    tb = tb_cpi.merge(tb_gdp, how="outer", on=["country", "year"])

    # Drop rows with no data.
    tb = tb.dropna(axis=0, subset=["cpi", "gdp"], how="all").reset_index(drop=True)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_meadow.save()
