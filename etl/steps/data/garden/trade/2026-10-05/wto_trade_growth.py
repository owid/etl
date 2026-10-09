"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Base year of the combined index (1800 = 1).
BASE_YEAR = 1800
# Year where the historical series ends and the WTO series (1950 = 100) takes over.
SPLICE_YEAR = 1950
# Earliest last year of WTO data we accept (the previous version ended in 2024).
MIN_LAST_YEAR_WTO = 2025


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets.
    ds_wto = paths.load_dataset("wto_trade_growth")
    ds_historic = paths.load_dataset("historic_trade")

    # Read tables from meadow datasets.
    tb_wto = ds_wto.read("wto_trade_growth")
    tb_historic = ds_historic.read("historic_trade")

    sanity_check_inputs(tb_wto=tb_wto, tb_historic=tb_historic)

    #
    # Process data.
    #

    # First, re-index historic data from 1913 = 100 to 1800 = 1
    baseline_1800_historic = tb_historic[tb_historic["year"] == BASE_YEAR]["volume_index"].iloc[0]
    tb_historic_reindexed = tb_historic.copy()
    tb_historic_reindexed["volume_index"] = tb_historic_reindexed["volume_index"] / baseline_1800_historic

    # Get the 1950 value from the re-indexed historic data (now 1800 = 1)
    baseline_1950_reindexed = tb_historic_reindexed[tb_historic_reindexed["year"] == SPLICE_YEAR]["volume_index"].iloc[
        0
    ]

    # Now scale WTO data (1950 = 100) to match the re-indexed scale (1800 = 1)
    tb_wto_adj = tb_wto.copy()
    tb_wto_adj["volume_index"] = tb_wto_adj["volume_index"] * baseline_1950_reindexed / 100

    # Combine: both are now on the same 1800 = 1 scale
    tb_combined = pr.concat(
        [
            tb_wto_adj[tb_wto_adj["year"] > SPLICE_YEAR],
            tb_historic_reindexed[tb_historic_reindexed["year"] <= SPLICE_YEAR],
        ],
        ignore_index=True,
    ).sort_values("year")

    sanity_check_outputs(tb_combined, tb_wto=tb_wto)

    # Combine the datasets
    tb_combined = tb_combined.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined], default_metadata=ds_wto.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb_wto, tb_historic) -> None:
    for name, tb in [("WTO", tb_wto), ("historic", tb_historic)]:
        assert set(tb["country"].unique()) == {"World"}, f"{name}: unexpected entities {set(tb['country'].unique())}"
        assert not tb["year"].duplicated().any(), f"{name}: duplicated years"
        assert (tb["volume_index"] > 0).all(), f"{name}: non-positive volume index values"

    # The WTO index must be based at 100 in the splice year, and the historic series must cover both anchor years.
    wto_splice = tb_wto.loc[tb_wto["year"] == SPLICE_YEAR, "volume_index"]
    assert len(wto_splice) == 1 and abs(wto_splice.iloc[0] - 100) < 1e-9, (
        f"WTO index is not {SPLICE_YEAR} = 100: {wto_splice.tolist()}"
    )
    missing_anchors = {BASE_YEAR, SPLICE_YEAR} - set(tb_historic["year"])
    assert not missing_anchors, f"Historic series lacks anchor years {missing_anchors}"

    # WTO years must be contiguous from the splice year onwards, and reach at least the expected last year.
    years_wto = sorted(tb_wto["year"].astype(int))
    assert years_wto == list(range(SPLICE_YEAR, years_wto[-1] + 1)), "WTO series has gaps in its years"
    assert years_wto[-1] >= MIN_LAST_YEAR_WTO, f"WTO series ends in {years_wto[-1]}, before {MIN_LAST_YEAR_WTO}"


def sanity_check_outputs(tb, tb_wto) -> None:
    assert not tb["year"].duplicated().any(), "Combined series has duplicated years"
    assert (tb["volume_index"] > 0).all(), "Combined series has non-positive values"
    base = tb.loc[tb["year"] == BASE_YEAR, "volume_index"]
    assert len(base) == 1 and abs(base.iloc[0] - 1) < 1e-9, f"Combined index is not {BASE_YEAR} = 1: {base.tolist()}"
    assert tb["year"].max() == tb_wto["year"].max(), "Combined series doesn't reach the last WTO year"

    # Rescaling must preserve the WTO's own year-on-year growth from the splice year onwards (including the splice).
    growth_combined = tb.set_index("year")["volume_index"].pct_change(fill_method=None).loc[SPLICE_YEAR + 1 :]
    growth_wto = (
        tb_wto.set_index("year")["volume_index"].sort_index().pct_change(fill_method=None).loc[SPLICE_YEAR + 1 :]
    )
    max_dev = (growth_combined - growth_wto).abs().max()
    assert max_dev < 1e-6, f"Combined series distorts WTO annual growth (max deviation {max_dev})"
