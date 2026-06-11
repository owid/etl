"""Load UN SDG meadow dataset and extract SP_ROD_R2KM (SDG 9.1.1) country-level data."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_un_sdg = paths.load_dataset("un_sdg", namespace="un", version="2025-10-29")
    tb = ds_un_sdg.read("un_sdg", reset_index=True)

    #
    # Process data.
    #
    # Keep only SP_ROD_R2KM rows (SDG 9.1.1 - rural population within 2 km of all-season road).
    tb = tb[tb["seriescode"] == "SP_ROD_R2KM"][["country", "year", "value"]].copy()
    tb = tb.format(["country", "year"], short_name="sdg_9_1_1")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=ds_un_sdg.metadata)
    ds_meadow.save()
