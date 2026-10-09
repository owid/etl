"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rio markers (and the environment policy marker) included in the totals: 10 (biodiversity), 20 (climate change
# mitigation), 30 (climate change adaptation), 40 (desertification), 50 (environment).
MARKERS = [10, 20, 30, 40, 50]
MARKER_MITIGATION = 20
MARKER_ADAPTATION = 30
# Scores of each project for each marker: 2 (principal objective), 1 (significant objective), 0 (screened, not
# targeted), and 99 or empty (not screened).
SCORE_PRINCIPAL = 2
SCORE_SIGNIFICANT = 1

# The adaptation marker applies to flows from 2010 onwards. Before that, every indicator that depends on it would only
# reflect mitigation, so it is removed.
START_YEAR_ADAPTATION_MARKER = 2010
# Rio markers were collected on a trial basis from 2004, and became a permanent part of the reporting in 2008.
START_YEAR_PERMANENT_MARKERS = 2008

# Codes of all developing countries (as recipients), all DAC members (DAC countries and EU institutions, as donors),
# and all sectors.
CODE_ALL_RECIPIENTS = "DPGC"
CODE_ALL_DONORS = "DAC_EC"
CODE_ALL_SECTORS = "1000"

# Groups of donors, defined in the OECD hierarchy of donors.
DONOR_GROUPS = [
    "DAC_EC",  # DAC members: DAC countries and EU institutions
    "DAC",  # DAC countries
    "DACEU",  # DAC EU countries
    "DACEU_EC",  # DAC EU countries and EU institutions
    "G7",  # G7 countries
]

# Codes ending in "_X" are aid to a region as a whole (e.g. "F_X", Africa, regional), or to developing countries in
# general ("DPGC_X"), that is not assigned to a single country. These are the exceptions: recipients that are not
# classified by income group.
CODES_UNCLASSIFIED_BY_INCOME = [
    "INC_X",  # Recipients not classified by income group (OECD)
    "INCWB_X",  # Countries not classified by the World Bank
]
# Annotation shown next to the names of entities whose aid is not assigned to a single country.
ANNOTATION_NOT_ASSIGNED = "not assigned to a single country"

# World Bank income groups of recipients, whose members change every year.
WORLD_BANK_INCOME_GROUPS = [
    "OLICWB",  # Low-income countries
    "LMICWB",  # Lower-middle-income countries
    "UMICWB",  # Upper-middle-income countries
    "HICSWB",  # High-income countries
    "INCWB_X",  # Countries not classified by the World Bank
]

# The OECD uses the same name for the country of Micronesia and the region of Micronesia (in Oceania).
NAMES_BY_CODE = {"O8": "Micronesia (OECD)"}

# Climate-related ODA, split into mutually exclusive categories in two ways: by Rio marker, and by objective.
CATEGORIES_MARKER = ["mitigation_only", "adaptation_only", "mitigation_and_adaptation"]
CATEGORIES_OBJECTIVE = ["climate_principal_objective", "climate_significant_objective"]
CLIMATE_COLUMNS = ["climate_related"] + CATEGORIES_MARKER + CATEGORIES_OBJECTIVE

# Indicators to publish for donors and for recipients.
INDICATORS_BY_DONOR = [
    "oda_bilateral_total",
    "oda_bilateral_allocable",
    "oda_bilateral_outside_rio_markers",
    *CLIMATE_COLUMNS,
    "non_climate",
    "climate_related_share",
    "climate_related_share_of_bilateral_total",
]
INDICATORS_BY_RECIPIENT = [
    "oda_bilateral_total",
    "oda_bilateral_allocable",
    "climate_related",
    "climate_related_share",
    "climate_related_share_of_bilateral_total",
]
# Indicators that do not depend on the adaptation marker, and are kept before it existed.
INDICATORS_WITHOUT_ADAPTATION = ["oda_bilateral_total", "oda_bilateral_allocable", "oda_bilateral_outside_rio_markers"]

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
    tb_bilateral = ds_meadow.read("bilateral_totals", safe_types=False)

    #
    # Process data.
    #
    for tb in [tb_totals, tb_activities, tb_bilateral]:
        for column in ["donor_code", "recipient_code", "sector_code"]:
            if column in tb.columns:
                tb[column] = tb[column].astype("string")

    sanity_check_inputs(tb_totals=tb_totals, tb_activities=tb_activities)

    # Classify climate-related activities into mutually exclusive categories, counting each activity only once.
    tb_climate = classify_climate_activities(tb_activities=tb_activities)

    # Define the donors and recipients (and their groups) to include, and the codes in the data that belong to each.
    # Individual donors: all donor codes except groups of donors (e.g. all DAC members).
    groups_donors = set(tb_groups[tb_groups["hierarchy"] == "donor"]["parent_code"])
    donors = sorted(set(tb_totals["donor_code"]) - groups_donors)
    members_donors = create_members(tb_groups=tb_groups, hierarchy="donor", entity_codes=donors + DONOR_GROUPS)
    recipients = sorted(set(tb_totals[tb_totals["donor_code"] == CODE_ALL_DONORS]["recipient_code"]))
    members_recipients = create_members(tb_groups=tb_groups, hierarchy="recipient", entity_codes=recipients)
    # Codes of groups of recipients (as opposed to individual recipients, which are their own only member).
    groups_recipients = set(tb_groups[tb_groups["hierarchy"] == "recipient"]["parent_code"])

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
    # Totals of each recipient and group of recipients (from all DAC members). Groups are computed from their members, so
    # they follow the latest hierarchy of recipients (the OECD's own group totals may use an older one).
    tb_totals_by_recipient = tb_totals[
        (tb_totals["donor_code"] == CODE_ALL_DONORS) & (tb_totals["sector_code"] == CODE_ALL_SECTORS)
    ]
    tb_totals_received = aggregate_by_members(
        tb=tb_totals_by_recipient[~tb_totals_by_recipient["recipient_code"].isin(groups_recipients)],
        code_column="recipient_code",
        members=members_recipients,
        keys=["marker", "score", "year"],
        columns=["value"],
    )
    sanity_check_recipient_groups(
        tb_computed=tb_totals_received,
        tb_oecd=tb_totals_by_recipient.rename(columns={"recipient_code": "entity_code"}),
        keys=["marker", "score", "year"],
        name="bilateral allocable ODA",
        expected_differences=WORLD_BANK_INCOME_GROUPS,
    )

    # Total bilateral ODA (including activities outside the scope of the Rio markers), from the CRS. Donor groups are
    # computed from individual donors, as above.
    tb_bilateral_given = aggregate_by_members(
        tb=tb_bilateral[
            (tb_bilateral["recipient_code"] == CODE_ALL_RECIPIENTS) & tb_bilateral["donor_code"].isin(donors)
        ],
        code_column="donor_code",
        members=members_donors,
        keys=["year"],
        columns=["value"],
    )
    sanity_check_donor_groups_bilateral(tb_bilateral_given=tb_bilateral_given, tb_bilateral=tb_bilateral)
    tb_bilateral_by_recipient = tb_bilateral[tb_bilateral["donor_code"] == CODE_ALL_DONORS]
    tb_bilateral_received = aggregate_by_members(
        tb=tb_bilateral_by_recipient[~tb_bilateral_by_recipient["recipient_code"].isin(groups_recipients)],
        code_column="recipient_code",
        members=members_recipients,
        keys=["year"],
        columns=["value"],
    )
    sanity_check_recipient_groups(
        tb_computed=tb_bilateral_received,
        tb_oecd=tb_bilateral_by_recipient.rename(columns={"recipient_code": "entity_code"}),
        keys=["year"],
        name="total bilateral ODA",
        # NOTE: In the CRS, the OECD's total for recipients not classified by income (INC_X) also includes developing
        # countries unspecified (DPGC_X), which its hierarchy (and the RioMarkers dataset) keep separate.
        expected_differences=WORLD_BANK_INCOME_GROUPS + ["INC_X"],
    )

    # Climate-related ODA given by each donor and group of donors, and received by each recipient and group.
    tb_given = create_climate_related_table(
        tb_climate=aggregate_by_members(
            tb=tb_climate, code_column="donor_code", members=members_donors, keys=["year"], columns=CLIMATE_COLUMNS
        ),
        tb_totals=tb_totals_given,
        tb_bilateral=tb_bilateral_given,
    )[["entity_code", "year"] + INDICATORS_BY_DONOR]
    tb_received = create_climate_related_table(
        tb_climate=aggregate_by_members(
            tb=tb_climate,
            code_column="recipient_code",
            members=members_recipients,
            keys=["year"],
            columns=CLIMATE_COLUMNS,
        ),
        tb_totals=tb_totals_received,
        tb_bilateral=tb_bilateral_received,
    )[["entity_code", "year"] + INDICATORS_BY_RECIPIENT]

    # Add names, harmonize them, and format tables.
    names = tb_codes[tb_codes["codelist"] == "area"].set_index("code")["name"].to_dict()
    names.update(NAMES_BY_CODE)
    tables = {"climate_related_oda_by_donor": tb_given, "climate_related_oda_by_recipient": tb_received}
    for short_name, tb in tables.items():
        tb["country"] = tb["entity_code"].map(names)
        assert tb["country"].notna().all(), f"Codes without names in {short_name}."

        # Harmonize country names.
        # NOTE: The same mapping covers donors and recipients, so each table uses only part of it.
        tb = paths.regions.harmonize_names(tb=tb, warn_on_unused_countries=False)

        # Names of recipients whose aid is not assigned to a single country.
        if short_name == "climate_related_oda_by_recipient":
            is_not_assigned = tb["entity_code"].str.endswith("_X") & ~tb["entity_code"].isin(
                CODES_UNCLASSIFIED_BY_INCOME
            )
            names_not_assigned = sorted(set(tb.loc[is_not_assigned, "country"]))
        tb = tb.drop(columns=["entity_code"])

        # Improve table format.
        tables[short_name] = tb.format(["country", "year"], short_name=short_name)

    sanity_check_outputs(tables=tables)
    sanity_check_not_assigned(names_not_assigned=names_not_assigned)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=list(tables.values()),
        default_metadata=ds_meadow.metadata,
        yaml_params={
            "ENTITY_ANNOTATIONS_RECIPIENTS": "\n".join(
                f"{name}: {ANNOTATION_NOT_ASSIGNED}" for name in names_not_assigned
            )
        },
    )

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

    tb = tb_activities.loc[keep, ["donor_code", "recipient_code", "year", "value"]].copy()
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
    return pr.read_from_records(rows, columns=["entity_code", "member_code"])


def aggregate_by_members(tb: Table, code_column: str, members: Table, keys: list[str], columns: list[str]) -> Table:
    """Sum columns over the members of each entity."""
    tb = tb.merge(members, left_on=code_column, right_on="member_code", how="inner")
    return tb.groupby(["entity_code"] + keys, as_index=False, observed=True)[columns].sum()


def create_climate_related_table(tb_climate: Table, tb_totals: Table, tb_bilateral: Table) -> Table:
    """Compare climate-related ODA (by entity and year) with bilateral allocable ODA and total bilateral ODA.

    Args:
        tb_climate: Climate-related ODA by entity and year.
        tb_totals: Totals by entity, marker, score and year.
        tb_bilateral: Total bilateral ODA by entity and year, including activities outside the scope of the Rio markers.
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

    # Express amounts in US dollars (the source is in millions).
    amounts = ["oda_bilateral_allocable"] + CLIMATE_COLUMNS + ["non_climate"]
    tb = tb[["entity_code", "year"] + amounts].copy()
    tb[amounts] *= 1e6
    tb["climate_related_share"] = share(tb["climate_related"], tb["oda_bilateral_allocable"])

    tb = add_total_bilateral_oda(tb=tb, tb_bilateral=tb_bilateral)

    # Remove indicators that depend on the adaptation marker before it existed.
    indicators = [
        column for column in tb.columns if column not in ["entity_code", "year"] + INDICATORS_WITHOUT_ADAPTATION
    ]
    tb.loc[tb["year"] < START_YEAR_ADAPTATION_MARKER, indicators] = None

    return tb


def add_total_bilateral_oda(tb: Table, tb_bilateral: Table) -> Table:
    """Add total bilateral ODA, the part of it outside the scope of the Rio markers, and climate-related ODA as a share of
    total bilateral ODA."""
    tb_bilateral = tb_bilateral.rename(columns={"value": "oda_bilateral_total"}, errors="raise")
    tb_bilateral["oda_bilateral_total"] *= 1e6
    tb = tb.merge(tb_bilateral, on=["entity_code", "year"], how="left")
    tb["oda_bilateral_outside_rio_markers"] = tb["oda_bilateral_total"] - tb["oda_bilateral_allocable"]

    # NOTE: Donors can only record cancellations of earlier commitments as one negative aggregate, outside the scope of
    # the Rio markers. So aid outside the scope of the Rio markers can be negative in a few donor-years (Australia in 2009
    # and 2010, and Denmark in 2019, also in the OECD's bulk files).
    negative = tb["oda_bilateral_outside_rio_markers"] < -1e4
    if negative.any():
        log.warning(
            "Negative bilateral ODA outside the scope of the Rio markers (net of cancellations) in: "
            f"{tb.loc[negative, ['entity_code', 'year']].values.tolist()}"
        )
    assert negative.sum() <= 10, (
        "Too many entity-years with negative bilateral ODA outside the scope of the Rio markers."
    )

    tb["climate_related_share_of_bilateral_total"] = share(tb["climate_related"], tb["oda_bilateral_total"])
    return tb


def pivot_totals(tb_totals: Table, marker: int, prefix: str) -> Table:
    """Totals of a marker by entity and year: targeted (principal or significant), and the total of all scores."""
    tb = tb_totals[tb_totals["marker"] == marker]
    tb_out = tb.groupby(["entity_code", "year"], as_index=False, observed=True)["value"].sum()
    tb_out = tb_out.rename(columns={"value": f"{prefix}_total"}, errors="raise")
    tb_related = (
        tb[tb["score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])]
        .groupby(["entity_code", "year"], as_index=False, observed=True)["value"]
        .sum()
        .rename(columns={"value": f"{prefix}_related"}, errors="raise")
    )
    tb_out = tb_out.merge(tb_related, on=["entity_code", "year"], how="left")
    tb_out[f"{prefix}_related"] = tb_out[f"{prefix}_related"].fillna(0)
    return tb_out


def share(numerator, denominator):
    """Numerator as a percentage of the denominator, left empty when the denominator is not positive."""
    return numerator / denominator.where(denominator > 0) * 100


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
    # NOTE: Before 2008, a few activities (e.g. about $290 million from Japan in 2003) have an invalid desertification
    # score ("3"), and the OECD leaves them out of the totals for desertification (also in its bulk files). So that
    # total is slightly lower than for the other markers in those years.
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

    # Activities must add up to the totals targeting each climate marker, for each donor and recipient.
    tb_targeted = tb_totals[
        tb_totals["score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])
        & tb_totals["marker"].isin([MARKER_MITIGATION, MARKER_ADAPTATION])
    ]
    for by, filters in [
        ("donor_code", {"recipient_code": CODE_ALL_RECIPIENTS, "sector_code": CODE_ALL_SECTORS}),
        ("recipient_code", {"donor_code": CODE_ALL_DONORS, "sector_code": CODE_ALL_SECTORS}),
    ]:
        mask = tb_targeted[list(filters)].eq(list(filters.values())).all(axis=1)
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


def sanity_check_donor_groups_bilateral(tb_bilateral_given: Table, tb_bilateral: Table) -> None:
    """All DAC members, computed from individual donors, must match the total bilateral ODA given by the OECD."""
    computed = tb_bilateral_given[tb_bilateral_given["entity_code"] == CODE_ALL_DONORS].set_index("year")["value"]
    oecd = tb_bilateral[
        (tb_bilateral["donor_code"] == CODE_ALL_DONORS) & (tb_bilateral["recipient_code"] == CODE_ALL_RECIPIENTS)
    ].set_index("year")["value"]
    computed, oecd = computed.align(oecd, join="inner")
    assert close(computed, oecd), "Total bilateral ODA of all DAC members does not match the OECD totals."


def sanity_check_recipient_groups(
    tb_computed: Table, tb_oecd: Table, keys: list[str], name: str, expected_differences: list[str]
) -> None:
    """Groups of recipients computed from their members must match the OECD's own group totals, except for the World Bank
    income groups, whose members change every year and which the OECD may compute with an older classification."""
    columns = ["entity_code"] + keys + ["value"]
    both = pr.merge(
        tb_computed[columns], tb_oecd[columns], on=columns[:-1], how="inner", suffixes=("_computed", "_oecd")
    )
    off = both[
        (both["value_computed"] - both["value_oecd"]).abs() > RELATIVE_TOLERANCE * both["value_oecd"].abs() + 0.01
    ]
    groups_off = set(off["entity_code"])
    if groups_off:
        log.warning(f"Groups of recipients whose {name} differs from the OECD's: {sorted(groups_off)}")
    assert groups_off <= set(expected_differences), f"Unexpected differences in {name} for: {sorted(groups_off)}."


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
        assert (shares.max() <= 100 + 1e-3).all(), f"Share above 100% in {name}."
        # Shares can only be slightly negative, because of cancelled commitments.
        if (shares.min() < 0).any():
            log.warning(f"Negative shares in {name}: {shares.min()[shares.min() < 0].to_dict()}")
        assert tb.columns[tb.isna().all()].empty, f"Fully empty indicator in {name}."

    # The same totals, given by all DAC members and received by all developing countries.
    given = tables["climate_related_oda_by_donor"].loc["DAC members"]
    received = tables["climate_related_oda_by_recipient"].loc["Developing countries (OECD)"]
    for column in ["climate_related", "oda_bilateral_allocable", "oda_bilateral_total"]:
        assert close(given[column].dropna(), received[column].dropna()), f"{column} given and received do not match."

    # Spot checks against values computed independently from the OECD bulk files (DAC members, 2023).
    row = tables["climate_related_oda_by_donor"].loc[("DAC members", 2023)]
    assert 55e9 < row["climate_related"] < 62e9, "Unexpected climate-related ODA given by DAC members in 2023."
    assert 30 < row["climate_related_share"] < 40, "Unexpected share of climate-related ODA given in 2023."
    assert 220e9 < row["oda_bilateral_total"] < 245e9, "Unexpected total bilateral ODA given by DAC members in 2023."


def close(a, b) -> bool:
    """Whether two series agree within a relative tolerance (plus a small absolute one, for values near zero)."""
    return bool(((a - b).abs() <= RELATIVE_TOLERANCE * b.abs() + 0.01).all())


def sanity_check_not_assigned(names_not_assigned: list[str]) -> None:
    """Recipients whose aid is not assigned to a single country must be named as such, so readers can tell them apart from
    countries and groups."""
    assert "Africa, regional (OECD)" in names_not_assigned, "Africa, regional is missing from the recipients."
    wrong = [n for n in names_not_assigned if not n.endswith((", regional (OECD)", ", unspecified (OECD)"))]
    assert not wrong, f"Recipients not assigned to a single country with unexpected names: {wrong}"
