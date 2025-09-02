"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_wdi_snapshot(snap):
    with snap.open_archive():
        file_name = [file.name for file in list(snap._unarchived_dir.glob("*")) if "Metadata" not in file.name][0]
        tb = snap.read_from_archive(filename=file_name, skiprows=4)

    return tb


def prepare_wdi_series(tb: Table, column_name: str) -> Table:
    tb = tb.copy()
    # Drop unnecessary columns and rename columns conveniently.
    tb = (
        tb.drop(columns=["Country Code", "Indicator Name", "Indicator Code"], errors="raise")
        .rename(columns={"Country Name": "country"}, errors="raise")
        .melt(id_vars=["country"], var_name="year", value_name=column_name)
    )

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load WDI auxiliary snapshots.
    snap_cpi = paths.load_snapshot("wdi_cpi.zip")
    snap_gdp = paths.load_snapshot("wdi_gdp.zip")
    snap_gdp_linked = paths.load_snapshot("wdi_gdp_linked.zip")
    tb_cpi = load_wdi_snapshot(snap=snap_cpi)
    tb_gdp = load_wdi_snapshot(snap=snap_gdp)
    tb_gdp_linked = load_wdi_snapshot(snap=snap_gdp_linked)

    #
    # Process data.
    #
    # Sanity check.
    error = "Downloaded snapshot file has changed format."
    assert set(tb_cpi["Indicator Code"]) == {"FP.CPI.TOTL"}, error
    assert set(tb_cpi["Indicator Name"]) == {"Consumer price index (2010 = 100)"}, error
    assert set(tb_gdp["Indicator Code"]) == {"NY.GDP.DEFL.ZS"}, error
    assert set(tb_gdp["Indicator Name"]) == {"GDP deflator (base year varies by country)"}, error
    assert set(tb_gdp_linked["Indicator Code"]) == {"NY.GDP.DEFL.ZS.AD"}, error
    assert set(tb_gdp_linked["Indicator Name"]) == {"GDP deflator: linked series (base year varies by country)"}, error

    # Prepare input series from WDI.
    tb_cpi = prepare_wdi_series(tb=tb_cpi, column_name="cpi")
    tb_gdp = prepare_wdi_series(tb=tb_gdp, column_name="gdp_deflator")
    tb_gdp_linked = prepare_wdi_series(tb=tb_gdp_linked, column_name="gdp_deflator_linked")

    # Combine tables.
    tb = tb_cpi.merge(tb_gdp, how="outer", on=["country", "year"])
    tb = tb.merge(tb_gdp_linked, how="outer", on=["country", "year"])

    # Drop rows with no data.
    tb = tb.dropna(axis=0, subset=tb.drop(columns=["country", "year"]).columns, how="all").reset_index(drop=True)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_meadow.save()
