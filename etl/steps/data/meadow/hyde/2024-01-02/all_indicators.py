"""Load a snapshot and create a meadow dataset."""

import os
from pathlib import Path

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Run step."""
    #
    # Load inputs.
    #
    paths.log.info("Loading snapshots...")
    ## All indicators
    snap = paths.load_snapshot("all_indicators.zip")
    ## General files (country codes)
    snap_codes = paths.load_snapshot("general_files.zip")

    # Load data from main snapshot
    tb = load_main_table(snap)
    # Load code -> country mapping table
    tb_codes = load_codes_table(snap_codes)

    #
    # Process main table
    #
    paths.log.info("Processing main table...")
    # Rename
    tb = tb.rename(columns={"region": "country"}, errors="raise")
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Process aux table
    #
    paths.log.info("Processing country codes table...")
    # Keep good columns
    tb_codes = tb_codes[["ISO-CODE", "Country"]]
    # Strip country names
    tb_codes["Country"] = tb_codes["Country"].str.strip()
    # Assert
    assert (tb_codes["ISO-CODE"] == 826).sum() == 3, "There should be three countries with ISO code 826!"
    assert "United Kingdom" in set(
        tb_codes[tb_codes["ISO-CODE"] == 826]["Country"]
    ), "United Kingdom should be in the list of countries with ISO code 826!"
    # Filter spurious entries
    mask = (tb_codes["ISO-CODE"] == 826) & (tb_codes["Country"] != "United Kingdom")
    tb_codes = tb_codes[~mask]
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_codes = tb_codes.underscore().set_index(["iso_code"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    paths.log.info("Saving outputs...")
    # Create list of tables
    tables = [
        tb,
        tb_codes,
    ]

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def load_main_table(snap: Snapshot) -> Table:
    """Load main table from snapshot."""
    with snap.extract_to_tempdir() as tmpdir:
        tbs = []
        files = os.listdir(tmpdir)
        for fname in files:
            # Sanity check
            assert Path(fname).suffix == ".txt", "All files in the directory should be .txt files!"
            # Only read country files
            if "_c.txt" in fname:
                # Read frame
                tb = pr.read_csv(
                    Path(tmpdir) / fname,
                    sep=" ",
                    metadata=snap.to_table_metadata(),
                    origin=snap.metadata.origin,
                )
                # Format frame
                tb = tb.melt(id_vars="region", var_name="year", value_name=fname.replace(".txt", ""))
                # Append frame to list of frames
                tbs.append(tb)

    # Merge all tables with metadata
    tb = tbs[0]
    for tb_ in tbs[1:]:
        tb = pr.merge(tb, tb_, how="outer", on=["region", "year"])

    return tb


def load_codes_table(snap: Snapshot) -> Table:
    """Load country codes table from snapshot.

    This table is later used to map country codes to country names.
    """
    with snap.extract_to_tempdir() as tmpdir:
        # Get all files in the directory
        files = os.listdir(tmpdir)
        # Sanity check
        fname_expected = "HYDE_country_codes.xlsx"
        assert fname_expected in files, f"The directory should contain a file named {fname_expected} (empty string)!"
        # Read file
        tb = pr.read_excel(
            Path(tmpdir) / fname_expected,
            metadata=snap.to_table_metadata(),
            origin=snap.metadata.origin,
        )
    return tb
