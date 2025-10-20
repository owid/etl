"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define old PIP regions, which will be phased out in future versions of the API
OLD_REGIONS = [
    "EAP",
    "ECA",
    "LAC",
    "MNA",
    "OHI",
    "SAR",
    "SSA",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    # For key indicators
    snap = paths.load_snapshot("world_bank_pip.csv")
    tb = snap.read(safe_types=False)

    # For percentiles
    snap_percentiles = paths.load_snapshot("world_bank_pip_percentiles.csv")
    tb_percentiles = snap_percentiles.read(safe_types=False)

    # For regional definitions
    snap_regions = paths.load_snapshot("world_bank_pip_regions.csv")
    tb_regions = snap_regions.read(safe_types=False)

    #
    # Process data.
    #

    # Make reporting_level and welfare_type strings
    tb["reporting_level"] = tb["reporting_level"].astype("string")
    tb["welfare_type"] = tb["welfare_type"].astype("string")
    tb_percentiles["reporting_level"] = tb_percentiles["reporting_level"].astype("string")
    tb_percentiles["welfare_type"] = tb_percentiles["welfare_type"].astype("string")

    # Drop old regions in tb
    tb = tb[~tb["country_code"].isin(OLD_REGIONS)].reset_index(drop=True)

    # Set index and sort
    tb = tb.format(["ppp_version", "filled", "poverty_line", "country", "year", "reporting_level", "welfare_type"])
    tb_percentiles = tb_percentiles.format(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type", "percentile"]
    )
    tb_regions = tb_regions.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb, tb_percentiles, tb_regions], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()
