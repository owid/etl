"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Names of the new indicators, for each (dimension, category) in the data.
INDICATORS = {
    ("enduse", "Direct human consumption"): "direct_human_consumption",
    ("enduse", "Fishmeal and fish oil"): "fishmeal_and_fish_oil",
    ("enduse", "Other"): "other_uses",
    ("gear", "bottom trawl"): "bottom_trawl",
    ("gear", "pelagic trawl"): "pelagic_trawl",
    ("gear", "purse seine"): "purse_seine",
    ("gear", "longline"): "longline",
    ("gear", "gillnets"): "gillnets",
    ("gear", "small scale"): "small_scale_gear",
    ("gear", "other"): "other_gear",
    ("gear", "unknown"): "unknown_gear",
    ("sector", "Industrial"): "industrial",
    ("sector", "Artisanal"): "artisanal",
    ("sector", "Subsistence"): "subsistence",
    ("sector", "Recreational"): "recreational",
    ("catchtype", "Landings"): "landings",
    ("catchtype", "Discards"): "discards",
    ("reporting-status", "Reported"): "reported_catch",
    ("reporting-status", "Unreported"): "unreported_catch",
}

# Indicators of each breakdown whose categories add up to the total catch (end uses add up to landings instead).
BREAKDOWNS_OF_TOTAL_CATCH = {
    "gear": [
        "bottom_trawl",
        "pelagic_trawl",
        "purse_seine",
        "longline",
        "gillnets",
        "small_scale_gear",
        "other_gear",
        "unknown_gear",
    ],
    "sector": ["industrial", "artisanal", "subsistence", "recreational"],
    "catchtype": ["landings", "discards"],
    "reporting-status": ["reported_catch", "unreported_catch"],
}
END_USES = ["direct_human_consumption", "fishmeal_and_fish_oil", "other_uses"]

# Last year with a usable breakdown by end use and by gear.
# NOTE: In the 2019 data, the end use and gear of several large fishing countries are left unallocated: Peru's
# anchoveta catch (mostly turned into fishmeal, and caught with purse seines) gets almost no fishmeal and goes to
# "unknown gear", and Indonesia and India also jump into "unknown gear". So we drop the end-use and gear breakdowns
# for that year. On the next update, check whether the latest year has the same problem (sanity_check_outputs
# catches it), and move this year forward.
LAST_YEAR_END_USE_AND_GEAR = 2018

# Tolerance (relative) when checking that the categories of different breakdowns add up to the same total.
# NOTE: The breakdowns of a few small fishing countries, in a few years, differ by up to ~1%.
RELATIVE_TOLERANCE = 0.02


def run() -> None:
    # Step 1: Load the meadow table.
    ds_meadow = paths.load_dataset("fish_catch")
    tb = ds_meadow.read("fish_catch")
    sanity_check_inputs(tb)

    # Step 2: Harmonize country names; "Global" becomes "World", and the catch of unknown fishing countries is
    # dropped from the country-level data (it is still part of the world total).
    tb = paths.regions.harmonize_names(tb)

    # Step 3: Add up fishing entities that belong to the same OWID country (e.g. Azores and Madeira are part of
    # Portugal, and Ascension and Tristan da Cunha are part of Saint Helena).
    tb = (
        tb.groupby(["country", "year", "dimension", "category"], observed=True)[["tonnes"]]
        .sum(min_count=1)
        .reset_index()
    )

    # Step 4: Create one indicator per dimension and category.
    tb["indicator"] = [INDICATORS[(d, c)] for d, c in zip(tb["dimension"], tb["category"])]
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="tonnes").reset_index()

    # Step 5: Add the total catch (landings plus discards).
    tb["total_catch"] = tb[BREAKDOWNS_OF_TOTAL_CATCH["catchtype"]].sum(axis=1, min_count=1)

    # Step 6: Drop the end-use and gear breakdowns of the latest year, which are incomplete (see NOTE above).
    tb.loc[tb["year"] > LAST_YEAR_END_USE_AND_GEAR, END_USES + BREAKDOWNS_OF_TOTAL_CATCH["gear"]] = None

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    # Step 7: Save the garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    # The data should only contain the (dimension, category) pairs that we know about.
    pairs = set(zip(tb["dimension"], tb["category"]))
    assert pairs == set(INDICATORS), f"New pairs: {pairs - set(INDICATORS)}; missing pairs: {set(INDICATORS) - pairs}"
    # Catch should never be negative.
    assert (tb["tonnes"].dropna() >= 0).all(), "Negative catch values."


def sanity_check_outputs(tb: Table) -> None:
    # The world total should be the sum of all fishing countries (including those that we don't show separately).
    # So it should be larger than the sum of the countries we keep, but not by much.
    tb_world = tb[tb["country"] == "World"].set_index("year")["total_catch"]
    tb_countries = tb[tb["country"] != "World"].groupby("year")["total_catch"].sum()
    ratio = tb_countries / tb_world
    assert ((ratio > 0.95) & (ratio <= 1 + 1e-6)).all(), f"Countries don't add up to the world: {ratio.describe()}"

    # Each breakdown should add up to the same total catch; end uses should add up to landings.
    for breakdown, columns in BREAKDOWNS_OF_TOTAL_CATCH.items():
        _sum = tb[columns].sum(axis=1, min_count=1)
        error = (_sum - tb["total_catch"]).abs() / tb["total_catch"]
        assert (error.dropna() < RELATIVE_TOLERANCE).all(), f"The {breakdown} breakdown doesn't add up to the total."
    error = (tb[END_USES].sum(axis=1, min_count=1) - tb["landings"]).abs() / tb["landings"]
    assert (error.dropna() < RELATIVE_TOLERANCE).all(), "End uses don't add up to landings."

    # Catch of unknown gear has always been less than 10% of the world's total catch; a jump above that means the
    # gear of some large fishing countries was not allocated (as in the 2019 data).
    tb_world_all = tb[tb["country"] == "World"].set_index("year")
    share_unknown_gear = tb_world_all["unknown_gear"] / tb_world_all["total_catch"]
    assert (share_unknown_gear.dropna() < 0.12).all(), f"Unexpected share of unknown gear: {share_unknown_gear.max()}"

    # Peru's catch is mostly anchoveta, turned into fishmeal and fish oil: in years with a large catch (over 2 million
    # tonnes), at least 70% of it has gone to fishmeal. A much lower share means its end use was not allocated.
    tb_peru = tb[(tb["country"] == "Peru") & (tb["landings"] > 2e6)]
    share_fishmeal_peru = (tb_peru["fishmeal_and_fish_oil"] / tb_peru["landings"]).dropna()
    assert (share_fishmeal_peru > 0.6).all(), f"Unexpected share of fishmeal in Peru: {share_fishmeal_peru.min()}"

    # World totals should be in a plausible range (in tonnes): roughly 20 million in 1950, and about 100 million today.
    assert tb_world.loc[1950] > 1e7 and tb_world.loc[1950] < 4e7, (
        f"Unexpected world total in 1950: {tb_world.loc[1950]}"
    )
    assert (tb_world.loc[2000:] > 8e7).all() and (tb_world.loc[2000:] < 1.5e8).all(), "Unexpected recent world total."
