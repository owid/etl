"""Load UN SDG meadow dataset and extract EN_URB_OPENSP (SDG 11.7.1) city-level data."""

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
    # Keep only city-level rows for EN_URB_OPENSP (exclude national totals and "no city" entries).
    tb = tb[(tb["seriescode"] == "EN_URB_OPENSP") & (~tb["cities"].isin(["_T", "NOCITI"]))]
    tb = tb[["country", "year", "cities", "value"]].copy()
    tb = tb.format(["country", "cities", "year"], short_name="sdg_11_7_1")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=ds_un_sdg.metadata)
    ds_meadow.save()
