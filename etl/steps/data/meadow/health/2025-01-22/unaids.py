"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve data from snapshots (~700 MB)
    tb_epi = paths.read_snap_table("unaids_epi.csv")  # 2,103,487 rows, 551 MB
    tb_gam = paths.read_snap_table("unaids_gam.csv")  # 166,550 rows, 42 MB
    tb_kpa = paths.read_snap_table("unaids_kpa.csv")  # 38,240 rows, 10 MB
    tb_ncpi = paths.read_snap_table("unaids_ncpi.csv")  # 263,428 rows, 80 MB

    #
    # Process data.
    #
    # Split columns of type dictionary in multiple columns
    paths.log.info("health.unaids: basic table cleaning")
    tb_epi = clean_table(tb_epi)  # 360 MB
    tb_gam = clean_table(tb_gam)  # 25 MB
    tb_kpa = clean_table(tb_kpa)  # 7.5 MB
    tb_ncpi = clean_table(tb_ncpi)  # 67 MB

    # TODO: Check is_text=True what is the value and correct
    # tb_kpa[tb_kpa["is_text"]].value.unique()
    # tb_kpa[(tb_kpa.value=="0") & (tb_kpa["is_text"])]

    # Remove unwanted indicators
    # id_desc_rm = [
    #     "National AIDS strategy/policy",
    #     "National AIDS strategy/policy includes dedicated budget for gender transformative interventions",
    # ]
    # tb = tb[~tb["indicator_description"].isin(id_desc_rm)]
    # # Type
    # tb = tb.astype(
    #     {
    #         "obs_value": float,
    #     }
    # )

    # Format
    tables = [
        tb_epi.format(["country", "year", "indicator", "dimension"], short_name="epi"),
        tb_gam.format(["country", "year", "indicator", "dimension"], short_name="gam"),
        tb_kpa.format(["country", "year", "indicator", "dimension"], short_name="kpa"),
        tb_ncpi.format(["country", "year", "indicator", "dimension"], short_name="ncpi"),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()


def clean_table(tb):
    """Minor table cleaning."""
    paths.log.info(f"Formatting table {tb.m.short_name}")

    # Rename columns, only keep relevant
    columns = {
        "TIME_PERIOD": "year",
        "INDICATOR.id": "indicator",
        "INDICATOR.value": "indicator_description",
        "SUBGROUP.id": "dimension",
        "SUBGROUP.value": "dimension_name",
        # "AREA.id": "code",
        "AREA.value": "country",
        "UNIT.id": "unit",
        "SOURCE": "source",
        "IS_TEXTUALDATA": "is_text",
        "OBS_VALUE": "value",
    }
    tb = tb.rename(columns=columns)[columns.values()]

    # Drop duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "indicator", "dimension"], keep="first")

    # Handle NaNs
    tb["value"] = tb["value"].replace("...", np.nan)
    tb = tb.dropna(subset=["value"])

    return tb
