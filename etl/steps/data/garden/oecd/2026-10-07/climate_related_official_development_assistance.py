"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rio markers (and the environment policy marker), with their names.
MARKERS = {
    10: "Biodiversity",
    20: "Climate change mitigation",
    30: "Climate change adaptation",
    40: "Desertification",
    50: "Environment",
}
MARKER_MITIGATION = 20
MARKER_ADAPTATION = 30
SCORE_PRINCIPAL = 2
SCORE_SIGNIFICANT = 1
SCORE_NOT_SCREENED = 99

# The adaptation marker applies to flows from 2010 onwards. Before that, every indicator that depends on it would only
# reflect mitigation, so it is removed.
START_YEAR_ADAPTATION_MARKER = 2010
# Rio markers were collected on a trial basis from 2004, and became a permanent part of the reporting in 2008.
START_YEAR_PERMANENT_MARKERS = 2008

# Codes of all developing countries, all DAC members, and all sectors.
CODE_ALL_RECIPIENTS = "DPGC"
CODE_ALL_DONORS = "DAC_EC"
CODE_ALL_SECTORS = "1000"

# Groups of donors, defined in the OECD hierarchy of donors.
DONOR_GROUPS = ["DAC_EC", "DAC", "DACEU", "DACEU_EC", "G7"]

# Codes of recipients that are kept: everything except regional "unspecified" codes (e.g. "Africa, regional"), which
# are not countries or groups.
CODES_UNSPECIFIED_KEPT = ["DPGC_X", "INC_X", "INCWB_X"]

# The OECD uses the same name for the country of Micronesia and the region of Micronesia (in Oceania).
NAMES_BY_CODE = {"O8": "Micronesia (OECD)"}

# Sectors (codes of the DAC sector classification), with names that follow those of our main ODA dataset.
SECTORS = {
    "1000": "Total",
    "450": "Total sector allocable",
    "100": "Social infrastructure and services",
    "110": "Education",
    "120": "Health",
    "130": "Population policies/programmes and reproductive health",
    "140": "Water supply and sanitation",
    "150": "Government and civil society",
    "160": "Other social infrastructure and services",
    "200": "Economic infrastructure and services",
    "210": "Transport and storage",
    "220": "Communications",
    "230": "Energy",
    "240": "Banking and financial services",
    "250": "Business and other services",
    "300": "Production sectors",
    "310": "Agriculture, forestry, fishing",
    "320": "Industry, mining, construction",
    "331": "Trade policies and regulations",
    "332": "Tourism",
    "400": "Multi-sector / Cross-cutting",
    "410": "General environment protection",
    "430": "Other multisector",
    "500": "Commodity aid / General programme assistance",
    "520": "Development food assistance",
    "530": "Other commodity assistance",
    "600": "Action relating to debt",
    "700": "Humanitarian aid",
    "720": "Emergency response",
    "730": "Reconstruction relief and rehabilitation",
    "740": "Disaster prevention and preparedness",
    "998": "Unallocated / Unspecified",
}

# Climate-related ODA, split into mutually exclusive categories in two ways: by Rio marker, and by objective.
CATEGORIES_MARKER = ["mitigation_only", "adaptation_only", "mitigation_and_adaptation"]
CATEGORIES_OBJECTIVE = ["climate_principal_objective", "climate_significant_objective"]
CLIMATE_COLUMNS = ["climate_related"] + CATEGORIES_MARKER + CATEGORIES_OBJECTIVE

# Indicators of climate-related ODA received, also expressed per person.
INDICATORS_PER_CAPITA = ["oda_bilateral_allocable", "climate_related"] + CATEGORIES_MARKER

# Relative tolerance when reconciling sums (values are stored as float32).
RELATIVE_TOLERANCE = 1e-4


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("climate_related_official_development_assistance")

    # Read tables from meadow dataset.
    tb_totals = ds_meadow.read("totals", safe_types=False)
    tb_activities = ds_meadow.read("activities", safe_types=False)
    tb_codes = ds_meadow.read("codes")
    tb_groups = ds_meadow.read("groups")

    #
    # Process data.
    #
    for tb in [tb_totals, tb_activities]:
        for column in ["donor_code", "recipient_code", "sector_code"]:
            tb[column] = tb[column].astype("string")

    sanity_check_inputs(tb_totals=tb_totals, tb_activities=tb_activities)

    # Classify climate-related activities into mutually exclusive categories, counting each activity only once.
    tb_climate = classify_climate_activities(tb_activities=tb_activities)

    # Define the donors and recipients (and their groups) to include, and the codes in the data that belong to each.
    donors = sorted(set(tb_totals["donor_code"]) - {CODE_ALL_DONORS})
    members_donors = create_members(tb_groups=tb_groups, hierarchy="donor", entity_codes=donors + DONOR_GROUPS)
    recipients = sorted(
        code
        for code in set(tb_totals[tb_totals["donor_code"] == CODE_ALL_DONORS]["recipient_code"])
        if not code.endswith("_X") or code in CODES_UNSPECIFIED_KEPT
    )
    members_recipients = create_members(tb_groups=tb_groups, hierarchy="recipient", entity_codes=recipients)

    # Totals of each donor and group of donors (to all developing countries, all sectors), computed from the totals of
    # individual donors. Totals of each recipient and group of recipients (from all DAC members), as given by the OECD.
    tb_totals_given = aggregate_by_members(
        tb=tb_totals[
            (tb_totals["recipient_code"] == CODE_ALL_RECIPIENTS)
            & (tb_totals["sector_code"] == CODE_ALL_SECTORS)
            & (tb_totals["donor_code"].isin(donors))
        ],
        code_column="donor_code",
        members=members_donors,
        keys=["marker", "score", "year"],
        columns=["value"],
    )
    sanity_check_donor_groups(tb_totals_given=tb_totals_given, tb_totals=tb_totals)
    tb_totals_received = tb_totals[
        (tb_totals["donor_code"] == CODE_ALL_DONORS)
        & (tb_totals["sector_code"] == CODE_ALL_SECTORS)
        & (tb_totals["recipient_code"].isin(recipients))
    ].rename(columns={"recipient_code": "entity_code"})[["entity_code", "marker", "score", "year", "value"]]

    # Climate-related ODA given by each donor and group of donors, and received by each recipient and group.
    tb_given = create_climate_related_table(
        tb_climate=aggregate_by_members(
            tb=tb_climate, code_column="donor_code", members=members_donors, keys=["year"], columns=CLIMATE_COLUMNS
        ),
        tb_totals=tb_totals_given,
    )
    tb_received = create_climate_related_table(
        tb_climate=aggregate_by_members(
            tb=tb_climate,
            code_column="recipient_code",
            members=members_recipients,
            keys=["year"],
            columns=CLIMATE_COLUMNS,
        ),
        tb_totals=tb_totals_received,
    )

    # ODA targeting each Rio marker (one marker at a time), given and received.
    tb_markers_given = create_markers_table(tb_totals=tb_totals_given)
    tb_markers_received = create_markers_table(tb_totals=tb_totals_received)

    # Climate-related ODA given by each donor and group of donors, by sector.
    tb_sectors = create_sectors_table(
        tb_climate=tb_climate, tb_totals=tb_totals, tb_codes=tb_codes, members_donors=members_donors, donors=donors
    )

    # Add names, harmonize them, and format tables.
    names = tb_codes[tb_codes["codelist"] == "area"].set_index("code")["name"].to_dict()
    names.update(NAMES_BY_CODE)
    groups_of_members = [
        code
        for members in [members_donors, members_recipients]
        for code, group in members.groupby("entity_code")
        if set(group["member_code"]) != {code}
    ]
    tables = {
        "climate_related_oda_by_donor": (tb_given, []),
        "climate_related_oda_by_recipient": (tb_received, []),
        "rio_markers_oda_by_donor": (tb_markers_given, ["marker"]),
        "rio_markers_oda_by_recipient": (tb_markers_received, ["marker"]),
        "climate_related_oda_by_donor_and_sector": (tb_sectors, ["sector"]),
    }
    for short_name, (tb, dimensions) in tables.items():
        tb["country"] = tb["entity_code"].map(names)
        assert tb["country"].notna().all(), f"Codes without names in {short_name}."
        # Groups are the entities whose members are other codes.
        tb["is_group"] = tb["entity_code"].isin(set(groups_of_members))
        tb = tb.drop(columns=["entity_code"])

        # Harmonize country names.
        # NOTE: The same mapping covers donors and recipients, so each table uses only part of it.
        tb = paths.regions.harmonize_names(tb=tb, warn_on_unused_countries=False)

        if short_name == "climate_related_oda_by_recipient":
            tb = add_indicators_per_capita(tb=tb)
        tb = tb.drop(columns=["is_group"])

        # Improve table format.
        tables[short_name] = tb.format(["country", "year"] + dimensions, short_name=short_name)

    sanity_check_outputs(tables=tables)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=list(tables.values()), default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def classify_climate_activities(tb_activities: Table) -> Table:
    """Return one row per climate-related activity, with its value split by marker category and by objective.

    Each activity marked for both mitigation and adaptation appears twice in the data (once per marker). To count it
    only once, keep all activities marked for mitigation, plus those marked for adaptation but not for mitigation.
    """
    is_mitigation = tb_activities["climate_mitigation_score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])
    is_adaptation = tb_activities["climate_adaptation_score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])
    is_principal = (
        (tb_activities["climate_mitigation_score"] == SCORE_PRINCIPAL)
        | (tb_activities["climate_adaptation_score"] == SCORE_PRINCIPAL)
    ).fillna(False)
    keep = (tb_activities["marker"] == MARKER_MITIGATION) | (
        (tb_activities["marker"] == MARKER_ADAPTATION) & ~is_mitigation
    )

    tb = tb_activities.loc[keep, ["donor_code", "recipient_code", "sector_code", "year", "value"]].copy()
    masks = {
        "mitigation_only": is_mitigation & ~is_adaptation,
        "adaptation_only": ~is_mitigation & is_adaptation,
        "mitigation_and_adaptation": is_mitigation & is_adaptation,
        "climate_principal_objective": is_principal,
        "climate_significant_objective": ~is_principal,
    }
    for category, mask in masks.items():
        tb[category] = tb["value"] * mask[keep].astype(int)
    tb = tb.rename(columns={"value": "climate_related"}, errors="raise")

    return tb


def create_members(tb_groups: Table, hierarchy: str, entity_codes: list[str]) -> Table:
    """Map each entity to the codes in the data that belong to it: itself for individual donors and recipients, and its
    members (at the lowest level of the hierarchy) for groups."""
    edges = tb_groups[tb_groups["hierarchy"] == hierarchy]
    children = edges.groupby("parent_code")["child_code"].apply(list).to_dict()

    def leaves(code: str) -> set[str]:
        if code not in children:
            return {code}
        return set().union(*(leaves(child) for child in children[code]))

    rows = [(code, member) for code in entity_codes for member in sorted(leaves(code))]
    return Table(pd.DataFrame(rows, columns=["entity_code", "member_code"]))


def aggregate_by_members(tb: Table, code_column: str, members: Table, keys: list[str], columns: list[str]) -> Table:
    """Sum columns over the members of each entity."""
    tb = tb.merge(members, left_on=code_column, right_on="member_code", how="inner")
    return tb.groupby(["entity_code"] + keys, as_index=False, observed=True)[columns].sum()


def create_climate_related_table(tb_climate: Table, tb_totals: Table) -> Table:
    """Compare climate-related ODA (by entity and year) with total bilateral allocable ODA.

    Args:
        tb_climate: Climate-related ODA by entity and year.
        tb_totals: Totals by entity, marker, score and year.
    """
    # Total bilateral allocable ODA (the sum of all scores of any marker), and totals targeting each climate marker.
    tb = pivot_totals(tb_totals=tb_totals, marker=MARKER_MITIGATION, prefix="mitigation")
    tb = tb.merge(
        pivot_totals(tb_totals=tb_totals, marker=MARKER_ADAPTATION, prefix="adaptation"),
        on=["entity_code", "year"],
        how="outer",
    )
    tb = tb.rename(columns={"mitigation_total": "oda_bilateral_allocable"}, errors="raise")

    # Combine with climate-related ODA. Entity-years without climate-related activities have zero climate-related ODA.
    assert set(tb_climate["entity_code"]) <= set(tb["entity_code"]), "Climate-related ODA for entities without totals."
    tb = tb.merge(tb_climate, on=["entity_code", "year"], how="left")
    tb[CLIMATE_COLUMNS] = tb[CLIMATE_COLUMNS].fillna(0)
    tb["non_climate"] = tb["oda_bilateral_allocable"] - tb["climate_related"]

    sanity_check_reconciliation(tb=tb)

    # Keep only the indicators to publish, in US dollars (the source is in millions).
    amounts = ["oda_bilateral_allocable"] + CLIMATE_COLUMNS + ["non_climate"]
    tb = tb[["entity_code", "year"] + amounts].copy()
    tb[amounts] *= 1e6

    # Shares of total bilateral allocable ODA.
    for column in CLIMATE_COLUMNS + ["non_climate"]:
        tb[f"{column}_share"] = share(tb[column], tb["oda_bilateral_allocable"])

    # Remove indicators that depend on the adaptation marker before it existed.
    indicators = [column for column in tb.columns if column not in ["entity_code", "year", "oda_bilateral_allocable"]]
    tb.loc[tb["year"] < START_YEAR_ADAPTATION_MARKER, indicators] = None

    return tb


def create_markers_table(tb_totals: Table) -> Table:
    """ODA targeting each marker (as a principal or significant objective), one marker at a time."""
    tables = []
    for marker, marker_name in MARKERS.items():
        tb = pivot_totals(tb_totals=tb_totals, marker=marker, prefix="marker")
        tb["marker"] = marker_name
        tables.append(tb)
    tb = pr.concat(tables, ignore_index=True)
    tb = tb.rename(
        columns={
            "marker_related": "oda_targeting_marker",
            "marker_principal": "oda_targeting_marker_principal",
            "marker_significant": "oda_targeting_marker_significant",
            "marker_total": "oda_bilateral_allocable",
        },
        errors="raise",
    )

    amounts = ["oda_targeting_marker", "oda_targeting_marker_principal", "oda_targeting_marker_significant"]
    for column in amounts:
        tb[f"{column}_share"] = share(tb[column], tb["oda_bilateral_allocable"])
    tb["not_screened_share"] = share(tb["marker_not_screened"], tb["oda_bilateral_allocable"])
    tb[amounts] *= 1e6
    tb = tb.drop(columns=["oda_bilateral_allocable", "marker_not_screened"])

    # The adaptation marker did not exist before 2010.
    indicators = [column for column in tb.columns if column not in ["entity_code", "year", "marker"]]
    is_before_adaptation = (tb["marker"] == MARKERS[MARKER_ADAPTATION]) & (tb["year"] < START_YEAR_ADAPTATION_MARKER)
    tb.loc[is_before_adaptation, indicators] = None

    return tb


def create_sectors_table(
    tb_climate: Table, tb_totals: Table, tb_codes: Table, members_donors: Table, donors: list[str]
) -> Table:
    """Climate-related ODA given by each donor and group of donors, by sector."""
    # Map each purpose code of the activities to the sectors it belongs to (its own sector and the broader groups).
    parents = tb_codes[tb_codes["codelist"] == "sector"].set_index("code")["parent_code"].to_dict()

    def sectors_of(code: str) -> list[str]:
        sectors = []
        while code is not None and not pd.isna(code):
            if code in SECTORS:
                sectors.append(code)
            code = parents.get(code)
        return sectors

    sector_codes = Table(
        pd.DataFrame(
            [(code, sector) for code in tb_climate["sector_code"].unique() for sector in sectors_of(code)],
            columns=["sector_code", "sector"],
        )
    )
    assert set(tb_climate["sector_code"]) <= set(sector_codes["sector_code"]), "Activities in unknown sectors."
    tb_climate = tb_climate.drop(columns=["recipient_code"]).merge(
        sector_codes, on="sector_code", how="inner", suffixes=("_activity", "")
    )
    tb_climate = tb_climate.drop(columns=["sector_code"])
    tb = aggregate_by_members(
        tb=tb_climate,
        code_column="donor_code",
        members=members_donors,
        keys=["sector", "year"],
        columns=CLIMATE_COLUMNS,
    )

    # Totals by sector (sum of all scores), and totals targeting each climate marker, by sector.
    tb_totals = tb_totals[
        (tb_totals["recipient_code"] == CODE_ALL_RECIPIENTS)
        & (tb_totals["donor_code"].isin(donors))
        & (tb_totals["sector_code"].isin(SECTORS))
        & (tb_totals["marker"].isin([MARKER_MITIGATION, MARKER_ADAPTATION]))
    ].rename(columns={"sector_code": "sector"})
    tb_totals = aggregate_by_members(
        tb=tb_totals,
        code_column="donor_code",
        members=members_donors,
        keys=["sector", "marker", "score", "year"],
        columns=["value"],
    )
    tb_totals["entity_code"] = tb_totals["entity_code"] + "|" + tb_totals["sector"]
    tb["entity_code"] = tb["entity_code"] + "|" + tb["sector"]
    tb = create_climate_related_table(
        tb_climate=tb.drop(columns=["sector"]), tb_totals=tb_totals.drop(columns="sector")
    )
    tb["sector"] = tb["entity_code"].str.split("|").str[1].map(SECTORS)
    tb["entity_code"] = tb["entity_code"].str.split("|").str[0]
    tb["sector"] = tb["sector"].copy_metadata(tb["climate_related"])
    tb["entity_code"] = tb["entity_code"].copy_metadata(tb["climate_related"])

    # Share of all climate-related ODA that goes to each sector.
    tb_all = tb[tb["sector"] == SECTORS[CODE_ALL_SECTORS]][["entity_code", "year", "climate_related"]]
    tb = tb.merge(tb_all, on=["entity_code", "year"], how="left", suffixes=("", "_all_sectors"))
    tb["climate_related_share_of_all_sectors"] = share(tb["climate_related"], tb["climate_related_all_sectors"])
    tb = tb.drop(
        columns=["climate_related_all_sectors", *CATEGORIES_OBJECTIVE, *[f"{c}_share" for c in CATEGORIES_OBJECTIVE]]
    )

    return tb


def pivot_totals(tb_totals: Table, marker: int, prefix: str) -> Table:
    """Totals of a marker by entity and year: targeted (principal or significant), principal, significant, not
    screened, and the total of all scores."""
    tb = tb_totals[tb_totals["marker"] == marker]
    columns = {
        "related": [SCORE_SIGNIFICANT, SCORE_PRINCIPAL],
        "principal": [SCORE_PRINCIPAL],
        "significant": [SCORE_SIGNIFICANT],
        "not_screened": [SCORE_NOT_SCREENED],
        "total": sorted(set(tb_totals["score"])),
    }
    tb_out = tb.groupby(["entity_code", "year"], as_index=False, observed=True)["value"].sum()[["entity_code", "year"]]
    for name, scores in columns.items():
        tb_score = (
            tb[tb["score"].isin(scores)]
            .groupby(["entity_code", "year"], as_index=False, observed=True)["value"]
            .sum()
            .rename(columns={"value": f"{prefix}_{name}"}, errors="raise")
        )
        tb_out = tb_out.merge(tb_score, on=["entity_code", "year"], how="left")
        tb_out[f"{prefix}_{name}"] = tb_out[f"{prefix}_{name}"].fillna(0)
    return tb_out


def share(numerator, denominator):
    """Numerator as a percentage of the denominator, left empty when the denominator is not positive."""
    return numerator / denominator.where(denominator > 0) * 100


def add_indicators_per_capita(tb: Table) -> Table:
    """Add indicators per person, only for countries.

    Groups of recipients are excluded: the OECD defines their members differently from the regions and income groups in
    our population data (e.g. it applies the current income classification to all years).
    """
    tb = paths.regions.add_population(tb=tb, population_col="population", warn_on_missing_countries=False)
    tb.loc[tb["is_group"], "population"] = None
    for column in INDICATORS_PER_CAPITA:
        tb[f"{column}_per_capita"] = tb[column] / tb["population"]
    return tb.drop(columns=["population"])


def sanity_check_inputs(tb_totals: Table, tb_activities: Table) -> None:
    # The marker attribute of each activity must match the marker it is listed under.
    for marker, column in [
        (MARKER_MITIGATION, "climate_mitigation_score"),
        (MARKER_ADAPTATION, "climate_adaptation_score"),
    ]:
        rows = tb_activities[tb_activities["marker"] == marker]
        assert (rows[column] == rows["score"]).all(), f"Score attribute does not match the score for marker {marker}."
    assert set(tb_activities["score"]) == {SCORE_SIGNIFICANT, SCORE_PRINCIPAL}, "Unexpected scores in activities."
    assert set(tb_totals["marker"]) == set(MARKERS), "Unexpected markers in totals."
    assert set(tb_activities["marker"]) == {MARKER_MITIGATION, MARKER_ADAPTATION}, "Unexpected markers in activities."

    # The total of bilateral allocable ODA must be the same for all markers.
    # NOTE: Before the markers became a permanent part of the reporting (2008), the total for desertification is
    # slightly lower in a few cases (at most 0.5% for all DAC members, in 2003). So the totals of each marker are always
    # computed from that marker.
    tb = tb_totals.groupby(["donor_code", "recipient_code", "sector_code", "year", "marker"], observed=True)[
        "value"
    ].sum()
    tb = tb.unstack()
    for marker in MARKERS:
        if marker != MARKER_MITIGATION:
            both = tb[[MARKER_MITIGATION, marker]].dropna()
            recent = both[both.index.get_level_values("year") >= START_YEAR_PERMANENT_MARKERS]
            assert close(recent[marker], recent[MARKER_MITIGATION]), (
                f"Total bilateral allocable ODA differs for {marker}."
            )
            if not close(both[marker], both[MARKER_MITIGATION]):
                log.warning(f"Total bilateral allocable ODA differs for marker {marker} before 2008.")

    # Activities must add up to the totals targeting each climate marker, for each donor, recipient and sector.
    tb_targeted = tb_totals[
        tb_totals["score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])
        & tb_totals["marker"].isin([MARKER_MITIGATION, MARKER_ADAPTATION])
    ]
    for by, filters in [
        ("donor_code", {"recipient_code": CODE_ALL_RECIPIENTS, "sector_code": CODE_ALL_SECTORS}),
        ("recipient_code", {"donor_code": CODE_ALL_DONORS, "sector_code": CODE_ALL_SECTORS}),
    ]:
        mask = pd.Series(True, index=tb_targeted.index)
        for column, code in filters.items():
            mask &= tb_targeted[column] == code
        totals = tb_targeted[mask].groupby([by, "marker", "year"], observed=True)["value"].sum()
        sums = tb_activities.groupby([by, "marker", "year"], observed=True)["value"].sum()
        # Compare only individual donors or recipients (not groups).
        totals = totals[totals.index.get_level_values(by).isin(set(tb_activities[by]))]
        totals, sums = totals.align(sums, fill_value=0)
        assert close(sums, totals), f"Activities do not add up to the totals by {by}."

    # Negative commitments (cancellations) are rare and small.
    share_negative = (tb_activities["value"] < 0).mean()
    assert share_negative < 0.001, f"Unexpectedly many negative activities ({share_negative:.2%})."


def sanity_check_donor_groups(tb_totals_given: Table, tb_totals: Table) -> None:
    """All DAC members, computed from individual donors, must match the totals given by the OECD."""
    keys = ["marker", "score", "year"]
    computed = tb_totals_given[tb_totals_given["entity_code"] == CODE_ALL_DONORS].set_index(keys)["value"]
    oecd = tb_totals[
        (tb_totals["donor_code"] == CODE_ALL_DONORS)
        & (tb_totals["recipient_code"] == CODE_ALL_RECIPIENTS)
        & (tb_totals["sector_code"] == CODE_ALL_SECTORS)
    ].set_index(keys)["value"]
    computed, oecd = computed.align(oecd, fill_value=0)
    assert close(computed, oecd), "All DAC members, computed from individual donors, do not match the OECD totals."


def sanity_check_reconciliation(tb: Table) -> None:
    """Check that the climate categories add up, and that they match the totals published by the OECD."""
    assert close(tb[CATEGORIES_MARKER].sum(axis=1), tb["climate_related"]), "Marker categories do not add up."
    assert close(tb[CATEGORIES_OBJECTIVE].sum(axis=1), tb["climate_related"]), "Objective categories do not add up."
    assert close(tb["mitigation_only"] + tb["mitigation_and_adaptation"], tb["mitigation_related"]), (
        "Mitigation-related activities do not match the OECD totals."
    )
    assert close(tb["adaptation_only"] + tb["mitigation_and_adaptation"], tb["adaptation_related"]), (
        "Adaptation-related activities do not match the OECD totals."
    )
    assert (tb["climate_related"] <= tb["oda_bilateral_allocable"] + 0.01).all(), "Climate-related ODA above the total."


def sanity_check_outputs(tables: dict[str, Table]) -> None:
    for name, tb in tables.items():
        shares = tb[[column for column in tb.columns if "_share" in column]]
        if name == "climate_related_oda_by_donor_and_sector":
            # NOTE: Cancelled commitments (negative values) can push the shares of small donors in a single sector
            # slightly above 100% (e.g. Slovenia's energy aid in 2019, or Denmark's unallocated aid in 2016).
            above = shares[shares.gt(100 + 1e-3).any(axis=1)]
            assert len(above) <= 5, f"Too many shares above 100% in {name}."
            if len(above) > 0:
                log.warning(f"Shares above 100% in {name}: {above.index.tolist()}")
        else:
            assert (shares.max() <= 100 + 1e-3).all(), f"Share above 100% in {name}."
        # Shares can only be slightly negative, because of cancelled commitments.
        if (shares.min() < 0).any():
            log.warning(f"Negative shares in {name}: {shares.min()[shares.min() < 0].to_dict()}")
        assert tb.columns[tb.isna().all()].empty, f"Fully empty indicator in {name}."

    # The same totals, given by all DAC members and received by all developing countries.
    given = tables["climate_related_oda_by_donor"].loc["DAC members"]
    received = tables["climate_related_oda_by_recipient"].loc["Developing countries (OECD)"]
    assert close(given["climate_related"].dropna(), received["climate_related"].dropna()), (
        "Climate-related ODA given and received do not match."
    )

    # Climate-related ODA by sector adds up to the total.
    # NOTE: A few activities (about $7 million over all years) are reported under administrative costs of donors, which
    # is not one of the sectors included, so the sum of sectors can be slightly below the total.
    tb = tables["climate_related_oda_by_donor_and_sector"].reset_index()
    tb = tb[tb["year"] >= START_YEAR_ADAPTATION_MARKER]
    total = tb[tb["sector"] == SECTORS[CODE_ALL_SECTORS]].set_index(["country", "year"])["climate_related"]
    parts = (
        tb[tb["sector"].isin([SECTORS[code] for code in ["450", "500", "600", "700", "998"]])]
        .groupby(["country", "year"])["climate_related"]
        .sum()
    )
    difference = (total.loc[parts.index] - parts).abs()
    assert (difference <= 0.01 * total.loc[parts.index].abs() + 5e6).all(), (
        "Climate-related ODA by sector does not add up to the total."
    )

    # Spot checks against values computed independently from the OECD Data Explorer (DAC members, 2023).
    row = tables["climate_related_oda_by_donor"].loc[("DAC members", 2023)]
    assert 55e9 < row["climate_related"] < 62e9, "Unexpected climate-related ODA given by DAC members in 2023."
    assert 30 < row["climate_related_share"] < 40, "Unexpected share of climate-related ODA given in 2023."


def close(a, b) -> bool:
    """Whether two series agree within a relative tolerance (plus a small absolute one, for values near zero)."""
    return bool(((a - b).abs() <= RELATIVE_TOLERANCE * b.abs() + 0.01).all())
