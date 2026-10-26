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

# The adaptation marker applies to flows from 2010 onwards. Before that, climate-related aid would only reflect
# mitigation, so all indicators start in 2010.
START_YEAR_ADAPTATION_MARKER = 2010
# Rio markers were collected on a trial basis from 2004, and became a permanent part of the reporting in 2008.
START_YEAR_PERMANENT_MARKERS = 2008

# Codes of all developing countries (as recipients), all DAC members (DAC countries and EU institutions, as donors),
# and all sectors.
CODE_ALL_RECIPIENTS = "DPGC"
CODE_ALL_DONORS = "DAC_EC"
CODE_ALL_SECTORS = "1000"

# The European Investment Bank (EIB), one of the EU institutions, reports its climate finance with a different method
# ("climate components"), not with Rio markers, so none of its aid is marked as climate-related. Like the OECD in its own
# climate-related shares, we leave it out of all totals, so its aid does not lower them.
CODE_EU_INSTITUTIONS = "4EU001"
AGENCY_EXCLUDED = "European Investment Bank"

# Codes of the DAC1 table: DAC countries (as a group), and measures of total ODA, bilateral ODA and contributions to EU
# institutions (EU institutions use the same code as in the RioMarkers data).
DAC1_DAC_COUNTRIES = "DAC"
DAC1_TOTAL_ODA = 1010
DAC1_BILATERAL_ODA = 1015
DAC1_CONTRIBUTIONS_TO_EU = 2102

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
    "climate_related_per_capita",
]
INDICATORS_BY_SECTOR = [
    "oda_bilateral_allocable",
    "climate_related",
    *CATEGORIES_MARKER,
    "climate_related_share",
    "climate_related_share_of_all_sectors",
]

# Sectors of the DAC classification that the Rio markers apply to, at the level that adds up to all sectors (each
# activity belongs to one of them), with the names of our main ODA dataset.
SECTORS = {
    "110": "Education",
    "120": "Health",
    "130": "Population policies/programmes and reproductive health",
    "140": "Water supply and sanitation",
    "150": "Government and civil society",
    "160": "Other social infrastructure and services",
    "210": "Transport and storage",
    "220": "Communications",
    "230": "Energy",
    "240": "Banking and financial services",
    "250": "Business and other services",
    "310": "Agriculture, forestry, fishing",
    "320": "Industry, mining, construction",
    "331": "Trade policies and regulations",
    "332": "Tourism",
    "410": "General environment protection",
    "430": "Other multisector",
    "520": "Development food assistance",
    "530": "Other commodity assistance",
    "600": "Action relating to debt",
    "720": "Emergency response",
    "730": "Reconstruction relief and rehabilitation",
    "740": "Disaster prevention and preparedness",
    "998": "Unallocated / Unspecified",
    "other": "Other",  # See CODE_OTHER_SECTORS
}
# NOTE: A few projects that the Rio markers apply to are in sectors that the snapshot does not query (general budget
# support and administrative costs of donors: at most 0.3% of bilateral allocable ODA, and a few million US$ of
# climate-related ODA). They are grouped as "Other", whose total is the total of all sectors minus the sectors above.
CODE_OTHER_SECTORS = "other"
SECTORS_IN_OTHER = ["510", "910"]

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
    tb_eu = ds_meadow.read("eu_institutions_activities")
    tb_codes = ds_meadow.read("codes")
    tb_groups = ds_meadow.read("groups")
    tb_bilateral = ds_meadow.read("bilateral_totals", safe_types=False)
    tb_dac1 = ds_meadow.read("total_oda")

    #
    # Process data.
    #
    # Use plain strings for codes, to compare them across tables.
    for tb in [tb_totals, tb_activities, tb_eu, tb_bilateral]:
        for column in ["donor_code", "recipient_code", "sector_code"]:
            if column in tb.columns:
                tb[column] = tb[column].astype("str")

    sanity_check_inputs(tb_totals=tb_totals, tb_activities=tb_activities, tb_eu=tb_eu)

    # Step 1: Climate-related aid, from project-level rows, counting each project once.
    tb_climate = classify_climate_activities(tb_activities=tb_activities)

    # Bilateral allocable ODA of the excluded agency (the EIB), by score, to subtract from the totals.
    tb_excluded = tb_eu[tb_eu["donor_agency"] == AGENCY_EXCLUDED].drop(columns=["donor_agency", "activity_id"])
    tb_excluded["marker"] = MARKER_MITIGATION

    # Step 2: Map each entity (donor, recipient or group) to its members in the data, to compute groups ourselves.
    # Individual donors: all donor codes except groups of donors (e.g. all DAC members).
    groups_donors = set(tb_groups[tb_groups["hierarchy"] == "donor"]["parent_code"])
    donors = sorted(set(tb_totals["donor_code"]) - groups_donors)
    # E.g. G7 -> CAN, DEU, FRA...; DEU -> DEU.
    members_donors = create_members(tb_groups=tb_groups, hierarchy="donor", entity_codes=donors + DONOR_GROUPS)
    recipients = sorted(set(tb_totals[tb_totals["donor_code"] == CODE_ALL_DONORS]["recipient_code"]))
    # E.g. Africa -> KEN, ETH, ..., F_X; KEN -> KEN.
    members_recipients = create_members(tb_groups=tb_groups, hierarchy="recipient", entity_codes=recipients)
    # Groups of recipients, whose OECD totals are dropped before summing (so they are not added to their members).
    groups_recipients = set(tb_groups[tb_groups["hierarchy"] == "recipient"]["parent_code"])

    # Step 3: Bilateral allocable ODA, from the RioMarkers totals: by donor (to all developing countries, all sectors).
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
    # Our total for all DAC members must match the OECD's.
    sanity_check_donor_groups(tb_totals_given=tb_totals_given, tb_totals=tb_totals)
    # Leave out the excluded agency (the EIB).
    tb_excluded_given = aggregate_by_members(
        tb=tb_excluded,
        code_column="donor_code",
        members=members_donors,
        keys=["marker", "score", "year"],
        columns=["value"],
    )
    tb_totals_given = subtract_excluded(
        tb=tb_totals_given, tb_excluded=tb_excluded_given, keys=["entity_code", "marker", "score", "year"]
    )
    # By recipient (from all DAC members, all sectors).
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
    # Our group totals must match the OECD's, except for income groups (whose members change over time).
    sanity_check_recipient_groups(
        tb_computed=tb_totals_received,
        tb_oecd=tb_totals_by_recipient.rename(columns={"recipient_code": "entity_code"}),
        keys=["marker", "score", "year"],
        name="bilateral allocable ODA",
        expected_differences=WORLD_BANK_INCOME_GROUPS,
    )
    tb_excluded_received = aggregate_by_members(
        tb=tb_excluded,
        code_column="recipient_code",
        members=members_recipients,
        keys=["marker", "score", "year"],
        columns=["value"],
    )
    tb_totals_received = subtract_excluded(
        tb=tb_totals_received, tb_excluded=tb_excluded_received, keys=["entity_code", "marker", "score", "year"]
    )

    # Step 4: Total bilateral ODA (including aid outside the scope of the Rio markers), from the CRS, as in step 3.
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
    # NOTE: All bilateral ODA of the EIB is allocable (in the CRS, its total bilateral ODA commitments equal its bilateral
    # allocable ODA here from 2011 on, and it reported only about $1 million a year in 2009-2010). So the same amounts are
    # left out of total bilateral ODA.
    tb_bilateral_given = subtract_excluded(
        tb=tb_bilateral_given, tb_excluded=sum_over_scores(tb_excluded_given), keys=["entity_code", "year"]
    )
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
    tb_bilateral_received = subtract_excluded(
        tb=tb_bilateral_received, tb_excluded=sum_over_scores(tb_excluded_received), keys=["entity_code", "year"]
    )

    # Step 5: All ODA of DAC members (DAC1), to show which share of it is bilateral.
    tb_total_oda = create_total_oda_table(
        tb_dac1=tb_dac1, tb_bilateral_given=tb_bilateral_given, tb_excluded=sum_over_scores(tb_excluded_given)
    )

    # Step 6: Climate-related ODA by donor and recipient, combined with the totals of steps 3 and 4 (for shares).
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
    )

    # Step 7: Climate-related ODA given by donor and sector, combined with bilateral allocable ODA in each sector.
    tb_sectors = create_sectors_table(
        tb_climate=tb_climate,
        tb_totals=tb_totals,
        tb_excluded=tb_excluded,
        tb_codes=tb_codes,
        members_donors=members_donors,
        donors=donors,
    )

    # Step 8: Replace codes with names (from the OECD's codelist), harmonize them, and format tables.
    names = tb_codes[tb_codes["codelist"] == "area"].set_index("code")["name"].to_dict()
    names.update(NAMES_BY_CODE)
    tables = {
        "climate_related_oda_by_donor": (tb_given, []),
        "climate_related_oda_by_recipient": (tb_received, []),
        "climate_related_oda_by_donor_and_sector": (tb_sectors, ["sector"]),
        "total_oda": (tb_total_oda, []),
    }
    for short_name, (tb, dimensions) in tables.items():
        tb["country"] = tb["entity_code"].map(names)
        assert tb["country"].notna().all(), f"Codes without names in {short_name}."

        # Harmonize country names.
        # NOTE: The same mapping covers donors and recipients, so each table uses only part of it.
        tb = paths.regions.harmonize_names(tb=tb, warn_on_unused_countries=False)

        if short_name == "climate_related_oda_by_recipient":
            # Recipients whose aid is not assigned to a single country (e.g. Africa, regional), to annotate in charts.
            is_not_assigned = tb["entity_code"].str.endswith("_X") & ~tb["entity_code"].isin(
                CODES_UNCLASSIFIED_BY_INCOME
            )
            names_not_assigned = sorted(set(tb.loc[is_not_assigned, "country"]))
            tb = add_climate_related_per_capita(tb=tb, groups=groups_recipients)[
                ["entity_code", "country", "year"] + INDICATORS_BY_RECIPIENT
            ]
        tb = tb.drop(columns=["entity_code"])

        # Improve table format.
        tables[short_name] = tb.format(["country", "year"] + dimensions, short_name=short_name)

    sanity_check_outputs(tables=tables)
    sanity_check_not_assigned(names_not_assigned=names_not_assigned)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset (passing the annotations of recipients to the .meta.yml file).
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
    # Each row has both climate scores of its project.
    is_mitigation = tb_activities["climate_mitigation_score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])
    is_adaptation = tb_activities["climate_adaptation_score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])
    is_principal = (
        (tb_activities["climate_mitigation_score"] == SCORE_PRINCIPAL)
        | (tb_activities["climate_adaptation_score"] == SCORE_PRINCIPAL)
    ).fillna(False)
    # One row per activity: its mitigation row if it has one, otherwise its adaptation row.
    keep = (tb_activities["marker"] == MARKER_MITIGATION) | (
        (tb_activities["marker"] == MARKER_ADAPTATION) & ~is_mitigation
    )

    tb = tb_activities.loc[keep, ["donor_code", "recipient_code", "sector_code", "year", "value"]].copy()
    # Put each value in the column of its category (and zero in the others).
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
    members (at the lowest level of the hierarchy) for groups. E.g. Africa (F) contains South of Sahara (F6), which
    contains Eastern Africa (F3), which contains Kenya (KEN): so F maps to KEN (among others), and KEN maps to itself.
    """
    # Direct members of each group, e.g. {"F": ["F4", "F6", "F_X"], ...}.
    edges = tb_groups[tb_groups["hierarchy"] == hierarchy]
    children = edges.groupby("parent_code")["child_code"].apply(list).to_dict()

    # Codes at the bottom of the hierarchy below a code (or the code itself, if it is not a group).
    def leaves(code: str) -> set[str]:
        if code not in children:
            return {code}
        return set().union(*(leaves(child) for child in children[code]))

    rows = [(code, member) for code in entity_codes for member in sorted(leaves(code))]
    return pr.read_from_records(rows, columns=["entity_code", "member_code"])


def aggregate_by_members(tb: Table, code_column: str, members: Table, keys: list[str], columns: list[str]) -> Table:
    """Sum columns over the members of each entity (e.g. a row of Germany counts towards Germany, G7 and DAC members)."""
    tb = tb.merge(members, left_on=code_column, right_on="member_code", how="inner")
    return tb.groupby(["entity_code"] + keys, as_index=False, observed=True)[columns].sum()


def create_climate_related_table(
    tb_climate: Table, tb_totals: Table, tb_bilateral: Table | None = None, keys: list[str] | None = None
) -> Table:
    """Compare climate-related ODA with bilateral allocable ODA and (if given) total bilateral ODA.

    Args:
        tb_climate: Climate-related ODA by keys.
        tb_totals: Totals by keys, marker and score.
        tb_bilateral: Total bilateral ODA by entity and year, including activities outside the scope of the Rio markers.
        keys: Columns that identify each row (entity and year by default).
    """
    keys = keys or ["entity_code", "year"]
    # Total bilateral allocable ODA (the sum of all scores of any marker), and totals targeting each climate marker.
    tb = pivot_totals(tb_totals=tb_totals, marker=MARKER_MITIGATION, prefix="mitigation", keys=keys)
    tb = tb.merge(
        pivot_totals(tb_totals=tb_totals, marker=MARKER_ADAPTATION, prefix="adaptation", keys=keys),
        on=keys,
        how="outer",
    )
    tb = tb.rename(columns={"mitigation_total": "oda_bilateral_allocable"}, errors="raise")

    # Combine with climate-related ODA. Rows without climate-related activities have zero climate-related ODA.
    assert tb_climate.set_index(keys).index.isin(tb.set_index(keys).index).all(), "Climate-related ODA without totals."
    tb = tb.merge(tb_climate, on=keys, how="left")
    tb[CLIMATE_COLUMNS] = tb[CLIMATE_COLUMNS].fillna(0)
    tb["non_climate"] = tb["oda_bilateral_allocable"] - tb["climate_related"]

    sanity_check_reconciliation(tb=tb)

    # Express amounts in US dollars (the source is in millions).
    amounts = ["oda_bilateral_allocable"] + CLIMATE_COLUMNS + ["non_climate"]
    tb = tb[keys + amounts].copy()
    tb[amounts] *= 1e6
    tb["climate_related_share"] = share(tb["climate_related"], tb["oda_bilateral_allocable"])

    if tb_bilateral is not None:
        tb = add_total_bilateral_oda(tb=tb, tb_bilateral=tb_bilateral)

    # Keep only years with the adaptation marker.
    tb = tb[tb["year"] >= START_YEAR_ADAPTATION_MARKER].reset_index(drop=True)

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


def create_total_oda_table(tb_dac1: Table, tb_bilateral_given: Table, tb_excluded: Table) -> Table:
    """All ODA commitments of DAC members (DAC countries and EU institutions), and the share of it that is bilateral.

    EU institutions count as a donor here, as in the rest of the dataset. DAC countries' contributions to EU institutions
    are part of the DAC countries' (multilateral) ODA, and EU institutions then commit them as their own ODA, so they are
    subtracted once to avoid counting them twice. The excluded agency (the EIB) is subtracted, as everywhere else.
    """

    def series(donor: str, measure: int) -> Table:
        tb = tb_dac1[(tb_dac1["donor_code"] == donor) & (tb_dac1["measure"] == measure)][["year", "value"]]
        return tb.rename(columns={"value": f"{donor}_{measure}"}, errors="raise")

    tb = series(DAC1_DAC_COUNTRIES, DAC1_TOTAL_ODA)
    for donor, measure in [
        (DAC1_DAC_COUNTRIES, DAC1_CONTRIBUTIONS_TO_EU),
        (DAC1_DAC_COUNTRIES, DAC1_BILATERAL_ODA),
        (CODE_EU_INSTITUTIONS, DAC1_TOTAL_ODA),
        (CODE_EU_INSTITUTIONS, DAC1_BILATERAL_ODA),
    ]:
        tb = pr.merge(tb, series(donor, measure), on="year", how="inner")

    # Bilateral ODA of all DAC members, from the CRS, after leaving out the excluded agency.
    tb_bilateral = tb_bilateral_given[tb_bilateral_given["entity_code"] == CODE_ALL_DONORS][["year", "value"]]
    tb = pr.merge(tb, tb_bilateral.rename(columns={"value": "bilateral"}), on="year", how="inner")
    tb = pr.merge(
        tb,
        tb_excluded[tb_excluded["entity_code"] == CODE_ALL_DONORS][["year", "value"]].rename(
            columns={"value": "excluded"}
        ),
        on="year",
        how="left",
    )
    tb["excluded"] = tb["excluded"].fillna(0)
    tb = tb[tb["year"] >= START_YEAR_ADAPTATION_MARKER].reset_index(drop=True)

    # Bilateral ODA in DAC1 must match the CRS (before leaving out the excluded agency), so the share compares like
    # with like.
    dac1_bilateral = (
        tb[f"{DAC1_DAC_COUNTRIES}_{DAC1_BILATERAL_ODA}"] + tb[f"{CODE_EU_INSTITUTIONS}_{DAC1_BILATERAL_ODA}"]
    )
    difference = (tb["bilateral"] + tb["excluded"]) / dac1_bilateral - 1
    assert (difference.abs() < 0.01).all(), (
        f"Bilateral ODA in DAC1 and the CRS differ by up to {difference.abs().max():.1%}."
    )

    tb["oda_total"] = (
        tb[f"{DAC1_DAC_COUNTRIES}_{DAC1_TOTAL_ODA}"]
        - tb[f"{DAC1_DAC_COUNTRIES}_{DAC1_CONTRIBUTIONS_TO_EU}"]
        + tb[f"{CODE_EU_INSTITUTIONS}_{DAC1_TOTAL_ODA}"]
        - tb["excluded"]
    )
    tb["oda_bilateral_share_of_total"] = share(tb["bilateral"], tb["oda_total"])
    assert tb["oda_bilateral_share_of_total"].between(75, 95).all(), "Unexpected bilateral share of all ODA."
    # Express amounts in US dollars (the source is in millions).
    tb["oda_total"] *= 1e6
    tb["entity_code"] = CODE_ALL_DONORS

    return tb[["entity_code", "year", "oda_total", "oda_bilateral_share_of_total"]]


def create_sectors_table(
    tb_climate: Table, tb_totals: Table, tb_excluded: Table, tb_codes: Table, members_donors: Table, donors: list[str]
) -> Table:
    """Climate-related ODA given by each donor and group of donors, by sector, and its share of each sector's aid."""
    # Map the purpose code of each activity to its sector.
    tb_climate = map_to_sectors(tb=tb_climate, tb_codes=tb_codes)

    # Climate-related ODA by donor (and group of donors) and sector.
    keys = ["entity_code", "sector_code", "year"]
    tb_climate = aggregate_by_members(
        tb=tb_climate, code_column="donor_code", members=members_donors, keys=keys[1:], columns=CLIMATE_COLUMNS
    )

    # Totals by donor and sector (to all developing countries), as for all sectors.
    tb_totals = tb_totals[
        (tb_totals["recipient_code"] == CODE_ALL_RECIPIENTS)
        & tb_totals["donor_code"].isin(donors)
        & tb_totals["sector_code"].isin(list(SECTORS) + [CODE_ALL_SECTORS])
        & tb_totals["marker"].isin([MARKER_MITIGATION, MARKER_ADAPTATION])
    ]
    tb_totals = add_other_sectors(tb_totals=tb_totals)
    keys_totals = ["sector_code", "marker", "score", "year"]
    tb_totals = aggregate_by_members(
        tb=tb_totals, code_column="donor_code", members=members_donors, keys=keys_totals, columns=["value"]
    )
    # Leave out the excluded agency (the EIB).
    tb_excluded = aggregate_by_members(
        tb=map_to_sectors(tb=tb_excluded, tb_codes=tb_codes),
        code_column="donor_code",
        members=members_donors,
        keys=keys_totals,
        columns=["value"],
    )
    tb_totals = subtract_excluded(tb=tb_totals, tb_excluded=tb_excluded, keys=["entity_code"] + keys_totals)

    tb = create_climate_related_table(tb_climate=tb_climate, tb_totals=tb_totals, keys=keys)
    # NOTE: The total of "Other" is a small difference between two large totals, too imprecise to divide by.
    tb.loc[tb["sector_code"] == CODE_OTHER_SECTORS, "climate_related_share"] = None

    # Share of all climate-related ODA of each donor that goes to each sector.
    tb["climate_related_share_of_all_sectors"] = share(
        tb["climate_related"], tb.groupby(["entity_code", "year"])["climate_related"].transform("sum")
    )
    tb["sector"] = tb["sector_code"].map(SECTORS).copy_metadata(tb["sector_code"])

    return tb[["entity_code", "sector", "year"] + INDICATORS_BY_SECTOR]


def map_to_sectors(tb: Table, tb_codes: Table) -> Table:
    """Replace the purpose code of each activity with its sector, up the hierarchy (e.g. 23210 -> 232 -> 230, energy)."""
    parents = tb_codes[tb_codes["codelist"] == "sector"].dropna(subset=["parent_code"])
    parents = parents.set_index("code")["parent_code"].to_dict()

    def sector_of(code: str) -> str | None:
        while code not in SECTORS and code not in SECTORS_IN_OTHER and code in parents:
            code = parents[code]
        if code in SECTORS_IN_OTHER:
            return CODE_OTHER_SECTORS
        return code if code in SECTORS else None

    sector_by_code = {code: sector_of(code) for code in tb["sector_code"].unique()}
    unknown = sorted(code for code, sector in sector_by_code.items() if sector is None)
    assert not unknown, f"Activities in purpose codes outside the expected sectors: {unknown}"
    tb = tb.copy()
    tb["sector_code"] = tb["sector_code"].map(sector_by_code)
    return tb


def subtract_excluded(tb: Table, tb_excluded: Table, keys: list[str]) -> Table:
    """Subtract the aid of the excluded agency (the EIB) from the totals with the same keys."""
    tb_excluded = tb_excluded.rename(columns={"value": "value_excluded"}, errors="raise")
    tb = tb.merge(tb_excluded, on=keys, how="left")
    # Every amount to subtract needs a total to subtract it from, which it can't exceed.
    assert tb["value_excluded"].notna().sum() == len(tb_excluded), "Aid of the excluded agency without a total."
    assert (tb["value"] - tb["value_excluded"] >= -1e-3).all(), "Aid of the excluded agency above the total."
    tb["value"] = tb["value"] - tb["value_excluded"].fillna(0)
    return tb.drop(columns=["value_excluded"])


def sum_over_scores(tb: Table) -> Table:
    """Sum the values of a marker over its scores, by entity and year (which gives bilateral allocable ODA)."""
    return tb.groupby(["entity_code", "year"], as_index=False, observed=True)["value"].sum()


def add_other_sectors(tb_totals: Table) -> Table:
    """Replace the totals of all sectors with those of the sectors not queried ("Other"): all sectors minus the rest."""
    keys = ["donor_code", "marker", "score", "year"]
    is_all = tb_totals["sector_code"] == CODE_ALL_SECTORS
    tb_listed = tb_totals[~is_all].groupby(keys, as_index=False, observed=True)["value"].sum()
    tb_other = tb_totals[is_all].merge(tb_listed, on=keys, how="left", suffixes=("_all", "_listed"))
    tb_other["value"] = tb_other["value_all"] - tb_other["value_listed"].fillna(0)
    tb_other["sector_code"] = CODE_OTHER_SECTORS

    # "Other" must be a small part of the total, from the year the data starts.
    recent = tb_other[tb_other["year"] >= START_YEAR_ADAPTATION_MARKER]
    assert recent["value"].abs().sum() < 0.01 * recent["value_all"].abs().sum(), "Too much aid in other sectors."

    return pr.concat([tb_totals[~is_all], tb_other[tb_totals.columns]], ignore_index=True)


def add_climate_related_per_capita(tb: Table, groups: set[str]) -> Table:
    """Add climate-related ODA received per person, only for countries.

    Groups of recipients are excluded: the OECD defines their members differently from the regions and income groups in
    our population data (e.g. it applies the current income classification to all years).
    """
    tb = paths.regions.add_population(tb=tb, population_col="population", warn_on_missing_countries=False)
    tb.loc[tb["entity_code"].isin(groups), "population"] = None
    tb["climate_related_per_capita"] = tb["climate_related"] / tb["population"]
    return tb.drop(columns=["population"])


def pivot_totals(tb_totals: Table, marker: int, prefix: str, keys: list[str]) -> Table:
    """Totals of a marker by keys: targeted (principal or significant), and the total of all scores."""
    tb = tb_totals[tb_totals["marker"] == marker]
    tb_out = tb.groupby(keys, as_index=False, observed=True)["value"].sum()
    tb_out = tb_out.rename(columns={"value": f"{prefix}_total"}, errors="raise")
    tb_related = (
        tb[tb["score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL])]
        .groupby(keys, as_index=False, observed=True)["value"]
        .sum()
        .rename(columns={"value": f"{prefix}_related"}, errors="raise")
    )
    tb_out = tb_out.merge(tb_related, on=keys, how="left")
    tb_out[f"{prefix}_related"] = tb_out[f"{prefix}_related"].fillna(0)
    return tb_out


def share(numerator, denominator):
    """Numerator as a percentage of the denominator, left empty when the denominator is not positive."""
    return numerator / denominator.where(denominator > 0) * 100


def sanity_check_inputs(tb_totals: Table, tb_activities: Table, tb_eu: Table) -> None:
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

    # Activities of EU institutions must add up to their totals, by score and year.
    totals = (
        tb_totals[
            (tb_totals["donor_code"] == CODE_EU_INSTITUTIONS)
            & (tb_totals["recipient_code"] == CODE_ALL_RECIPIENTS)
            & (tb_totals["sector_code"] == CODE_ALL_SECTORS)
            & (tb_totals["marker"] == MARKER_MITIGATION)
        ]
        .groupby(["score", "year"], observed=True)["value"]
        .sum()
    )
    sums = tb_eu.groupby(["score", "year"], observed=True)["value"].sum()
    totals, sums = totals.align(sums, fill_value=0)
    assert close(sums, totals), "Activities of EU institutions do not add up to their totals."
    # The excluded agency must be in the latest year, with none of its aid marked as climate-related (so it only needs
    # to be left out of the totals of all aid).
    excluded = tb_eu[tb_eu["donor_agency"] == AGENCY_EXCLUDED]
    assert excluded["year"].max() == tb_eu["year"].max(), f"No activities of {AGENCY_EXCLUDED} in the latest year."
    assert not excluded["score"].isin([SCORE_SIGNIFICANT, SCORE_PRINCIPAL]).any(), (
        f"Activities of {AGENCY_EXCLUDED} marked for climate change mitigation."
    )
    assert not (tb_activities["donor_agency"] == AGENCY_EXCLUDED).any(), (
        f"Activities of {AGENCY_EXCLUDED} marked as climate-related."
    )


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

    # Sectors add up to the totals of all sectors, for each donor and year.
    given_all = tables["climate_related_oda_by_donor"]
    by_sector = tables["climate_related_oda_by_donor_and_sector"].groupby(["country", "year"])
    for column in ["climate_related", "oda_bilateral_allocable"]:
        summed, total = by_sector[column].sum().align(given_all[column], join="inner")
        assert close(summed, total), f"Sectors do not add up to the total {column}."
    shares = by_sector["climate_related_share_of_all_sectors"].sum()[by_sector["climate_related"].sum() > 0]
    assert close(shares, shares * 0 + 100), "Shares of climate-related ODA by sector do not add up to 100%."
    assert set(tables["climate_related_oda_by_donor_and_sector"].index.get_level_values("sector")) == set(
        SECTORS.values()
    ), "Unexpected sectors."

    # All indicators start when the adaptation marker does.
    for name, tb in tables.items():
        assert tb.index.get_level_values("year").min() == START_YEAR_ADAPTATION_MARKER, f"Unexpected start in {name}."

    # Climate-related ODA received per person is only for countries, and within a plausible range (in US$).
    per_capita = tables["climate_related_oda_by_recipient"]["climate_related_per_capita"].dropna()
    assert not per_capita.index.get_level_values("country").str.contains("OECD").any(), "Per capita for groups."
    assert 50 < per_capita.index.get_level_values("country").nunique() < 200, "Unexpected number of countries."
    # NOTE: It is highest in small Pacific islands (e.g. about $7,600 in Niue, in 2024).
    assert per_capita.min() >= -1 and per_capita.max() < 20000, "Climate-related ODA per person out of range."

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
