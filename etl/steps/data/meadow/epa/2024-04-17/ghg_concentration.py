"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load data from snapshots.
    snap_co2 = paths.load_snapshot("co2_concentration.csv")
    snap_ch4 = paths.load_snapshot("ch4_concentration.csv")
    snap_n2o = paths.load_snapshot("n2o_concentration.csv")
    tb_co2 = snap_co2.read(skiprows=6)
    tb_ch4 = snap_ch4.read(skiprows=6)
    tb_n2o = snap_n2o.read(skiprows=6)

    #
    # Process data.
    #
    # Remove first row, which simply says "Ice core measurements".
    assert tb_co2.iloc[0, 0] == "Ice core measurements"
    assert tb_ch4.iloc[0, 0] == "Ice core measurements"
    assert tb_n2o.iloc[0, 0] == "Ice core measurements"
    tb_co2 = tb_co2.iloc[1:].reset_index(drop=True)
    tb_ch4 = tb_ch4.iloc[1:].reset_index(drop=True)
    tb_n2o = tb_n2o.iloc[1:].reset_index(drop=True)

    # For convenience, rename year column.
    tb_co2 = tb_co2.rename(columns={"Year": "year"}, errors="raise")
    tb_ch4 = tb_ch4.rename(columns={"Year (negative values = BC)": "year"}, errors="raise")
    tb_n2o = tb_n2o.rename(columns={"Year (negative values = BC)": "year"}, errors="raise")

    # Remove row that contains just the text "Direct measurements".
    tb_co2 = tb_co2[tb_co2["year"] != "Direct measurements"].reset_index(drop=True)
    tb_ch4 = tb_ch4[tb_ch4["year"] != "Direct measurements"].reset_index(drop=True)
    tb_n2o = tb_n2o[tb_n2o["year"] != "Direct measurements"].reset_index(drop=True)

    # Convert year column to a float.
    tb_co2["year"] = tb_co2["year"].astype(float)
    tb_ch4["year"] = tb_ch4["year"].astype(float)
    tb_n2o["year"] = tb_n2o["year"].astype(float)

    # Remove spurious empty row (with a repeated year 1988) in co2 concentration.
    tb_co2 = tb_co2.dropna(subset=[column for column in tb_co2.columns if column != "year"], how="all").reset_index(
        drop=True
    )

    # Improve tables format.
    tb_co2 = tb_co2.format(["year"], short_name="co2_concentration")
    tb_ch4 = tb_ch4.format(["year"], short_name="ch4_concentration")
    tb_n2o = tb_n2o.format(["year"], short_name="n2o_concentration")

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_co2, tb_ch4, tb_n2o], default_metadata=snap_co2.metadata)
    ds_meadow.save()
