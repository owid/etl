"""Load a meadow dataset and create a garden dataset."""

import structlog
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

historic_cols = [
    "decade",
    "country",
    "blue",
    "brydes",
    "fin",
    "gray",
    "humpback",
    "minke",
    "right",
    "sei",
    "sperm",
    "total",
    "unspecified_other",
    "decade_start",
]

iwc_cols = [
    "blue_whales_caught_total",
    "pygmy_blue_whales_caught_total",
    "fin_whales_caught_total",
    "sperm_whales_caught_total",
    "humpback_whales_caught_total",
    "sei_whales_caught_total",
    "brydes_whales_caught_total",
    "common_minke_whales_caught_total",
    "antarctic_minke_whales_caught_total",
    "gray_whales_caught_total",
    "bowhead_whales_caught_total",
    "right_whales_caught_total",
    "unspecified_large_whales_caught_total",
    "total_whales_caught_total",
]

# Mapping from IWC column names to historic (Rocha) column names.
# blue + pygmy_blue both map to "blue"; common + antarctic minke both map to "minke".
# bowhead has no historic equivalent and is excluded.
IWC_TO_HISTORIC = {
    "blue_whales_caught_total": "blue",
    "pygmy_blue_whales_caught_total": "blue",
    "brydes_whales_caught_total": "brydes",
    "fin_whales_caught_total": "fin",
    "gray_whales_caught_total": "gray",
    "humpback_whales_caught_total": "humpback",
    "common_minke_whales_caught_total": "minke",
    "antarctic_minke_whales_caught_total": "minke",
    "right_whales_caught_total": "right",
    "sei_whales_caught_total": "sei",
    "sperm_whales_caught_total": "sperm",
    "total_whales_caught_total": "total",
    "unspecified_large_whales_caught_total": "unspecified_other",
    "bowhead_whales_caught_total": "unspecified_other",  # no historic equivalent, map to "unspecified_other" to avoid losing data
}

LOG = structlog.get_logger()


def group_iwc_by_decade(tb_iwc):
    # Group IWC data by decade to compare with Rocha decade data.
    tb_iwc = tb_iwc.copy()
    tb_iwc["decade_start"] = (tb_iwc["year"] // 10) * 10
    tb_iwc_decades = tb_iwc.groupby(["decade_start", "country"]).sum().reset_index()
    # drop year column
    tb_iwc_decades = tb_iwc_decades.drop(columns=["year"])
    return tb_iwc_decades


def compare_iwc_vs_rocha(tb_iwc, tb_historic, years=[1990], decades=True, verbose=False):
    """Compare IWC and Rocha totals by species (and overall) for the given decades or years

    IWC columns are collapsed onto Rocha species names using IWC_TO_HISTORIC,
    then both sources are summed across all countries for each decade and put
    side-by-side so differences are easy to spot.
    """
    # Filter to relevant decades and only compare world totals (rocha doesn't have country breakdown)
    if decades:
        year_col = "decade_start"
    else:
        year_col = "year"

    tb_iwc = tb_iwc[tb_iwc[year_col].isin(years) & (tb_iwc["country"] == "World")]
    tb_historic = tb_historic[tb_historic[year_col].isin(years) & (tb_historic["country"] == "World")]

    # check totals match for each species and overall total
    for year in years:
        iwc_row = tb_iwc[tb_iwc[year_col] == year]
        rocha_row = tb_historic[tb_historic[year_col] == year]
        if iwc_row.empty or rocha_row.empty:
            LOG.warning(
                "compare_iwc_vs_rocha.missing_data",
                year=year,
                iwc_missing=iwc_row.empty,
                rocha_missing=rocha_row.empty,
            )
            continue
        species_cols = [c for c in historic_cols if c not in ("decade", "country", "decade_start", "year")]
        for historic_col in species_cols:
            iwc_val = iwc_row[historic_col].values[0]
            rocha_val = rocha_row[historic_col].values[0]
            if iwc_val != rocha_val and verbose:
                LOG.warning(
                    "compare_iwc_vs_rocha.mismatch",
                    year=year,
                    species=historic_col,
                    iwc_val=iwc_val,
                    rocha_val=rocha_val,
                )
            elif iwc_val == rocha_val and verbose:
                LOG.info(
                    "compare_iwc_vs_rocha.match",
                    year=year,
                    species=historic_col,
                    value=iwc_val,
                )


def map_iwc_to_historic(tb_iwc):
    # Map IWC columns to historic species columns using IWC_TO_HISTORIC.
    tb_iwc_mapped = tb_iwc.copy()
    # do special cases first
    tb_iwc_mapped["blue"] = tb_iwc["blue_whales_caught_total"] + tb_iwc["pygmy_blue_whales_caught_total"]
    tb_iwc_mapped["minke"] = tb_iwc["common_minke_whales_caught_total"] + tb_iwc["antarctic_minke_whales_caught_total"]
    tb_iwc_mapped["unspecified_other"] = (
        tb_iwc["unspecified_large_whales_caught_total"] + tb_iwc["bowhead_whales_caught_total"]
    )
    # then map the rest using the mapping dict
    for iwc_col, historic_col in IWC_TO_HISTORIC.items():
        if historic_col in ["blue", "minke", "unspecified_other"]:
            continue  # already handled in special cases
        tb_iwc_mapped[historic_col] = tb_iwc[iwc_col]
    # drop original IWC columns
    tb_iwc_mapped = tb_iwc_mapped.drop(columns=iwc_cols)
    return tb_iwc_mapped


def run() -> None:
    #
    # Load inputs.
    #
    ds_historic = paths.load_dataset("historic_whaling")
    ds_iwc = paths.load_dataset("whaling_total_catches")

    tb_iwc = ds_iwc.read("whaling_total_catches")
    tb_historic_decades = ds_historic.read("historic_whaling_decades")

    tb_historic_annual = ds_historic.read("historic_whaling_annual")

    tb_iwc_decades = group_iwc_by_decade(tb_iwc)
    # remove row "total"
    tb_historic_decades = tb_historic_decades[tb_historic_decades["decade"] != "total"]
    # change historic decade column to decade_start
    tb_historic_decades["decade_start"] = tb_historic_decades["decade"].apply(lambda x: int(x.split("-")[0]))
    tb_historic_decades = tb_historic_decades.drop(columns=["decade"])

    # prepare iwc data by mapping to historic species
    tb_iwc_mapped = map_iwc_to_historic(tb_iwc_decades)
    tb_iwc_mapped_annual = map_iwc_to_historic(tb_iwc)

    # check for mismatches between iwc and rocha decade totals for 1990s onwards (where they overlap)
    # only fyi, we expect some mismatches as rocha doesn't include substinence whaling
    LOG.info("compare_iwc_vs_rocha_decades", message="Comparing IWC vs Rocha decade totals for 1990s")
    compare_iwc_vs_rocha(tb_iwc_mapped, tb_historic_decades, years=[1990])

    LOG.info("compare_iwc_vs_rocha_annual", message="Comparing IWC vs Rocha annual totals from 1985-1998")
    compare_iwc_vs_rocha(tb_iwc_mapped_annual, tb_historic_annual, years=[y for y in range(1985, 2000)], decades=False)

    # combine IWC and Rocha decade data, using IWC where available (post 1990) and Rocha for pre 1990
    # save decade_start columns as int
    tb_historic_decades["decade_start"] = tb_historic_decades["decade_start"].astype(int)
    tb_iwc_mapped["decade_start"] = tb_iwc_mapped["decade_start"].astype(int)

    tb_combined_decades = pr.concat(
        [
            tb_historic_decades[tb_historic_decades["decade_start"] < 1990],
            tb_iwc_mapped[tb_iwc_mapped["decade_start"] >= 1990],
        ],
        ignore_index=True,
    )

    tb_combined_decades["year"] = tb_combined_decades["decade_start"]

    # combine IWC and Rocha annual data, using IWC where available (post 1985) and Rocha for pre 1985
    tb_historic_annual["year"] = tb_historic_annual["year"].astype(int)
    tb_iwc_mapped_annual["year"] = tb_iwc_mapped_annual["year"].astype(int)

    tb_combined_annual = pr.concat(
        [
            tb_historic_annual[tb_historic_annual["year"] < 1985],
            tb_iwc_mapped_annual[tb_iwc_mapped_annual["year"] >= 1985],
        ],
        ignore_index=True,
    )

    tb_combined_decades = tb_combined_decades.format(["year", "country"], short_name="long_run_whaling_decades").drop(
        columns=["decade_start"]
    )
    tb_combined_annual = tb_combined_annual.format(["year", "country"], short_name="long_run_whaling_annual")
    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb_combined_decades, tb_combined_annual], default_metadata=ds_historic.metadata
    )
    ds_garden.save()
