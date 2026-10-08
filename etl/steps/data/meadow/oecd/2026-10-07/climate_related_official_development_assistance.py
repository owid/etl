"""Load a snapshot and create a meadow dataset."""

import xml.etree.ElementTree as ET
import zipfile

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Base year of the OECD's constant-price series.
# NOTE: The OECD rebases it at every release, so when the assertion on it fails, bump BOTH this constant and
# `definitions.inflation_year` in the garden .meta.yml.
CONSTANT_PRICE_BASE_YEAR = 2024

# Dimensions that the snapshot queries fix to a single value (see the snapshot scripts for all dimensions and codes).
EXPECTED_FIXED_VALUES = {
    "MEASURE": 100,  # Official development assistance (ODA)
    "ALLOCABLE": 2,  # Bilateral allocable aid (assigned to a recipient and a sector)
    "FLOW_TYPE": "C",  # Commitments
    "PRICE_BASE": "Q",  # Constant prices
    "UNIT_MEASURE": "USD",  # US dollars
    "BASE_PER": CONSTANT_PRICE_BASE_YEAR,  # Base year of constant prices
    "UNIT_MULT": 6,  # Unit multiplier: values are in millions (10^6)
}

# The same, for the totals of bilateral ODA from the Creditor Reporting System (CRS), which has no "allocable" dimension
# but has channel and modality dimensions.
EXPECTED_FIXED_VALUES_CRS = {
    **{column: value for column, value in EXPECTED_FIXED_VALUES.items() if column != "ALLOCABLE"},
    "SECTOR": 1000,  # All sectors
    "CHANNEL": "_T",  # All channels of delivery
    "MODALITY": "_T",  # All types of aid
}

# Columns to keep from the data files, and their new names.
COLUMNS_TOTALS = {
    "DONOR": "donor_code",
    "RECIPIENT": "recipient_code",
    "SECTOR": "sector_code",
    "MARKER": "marker",
    "SCORE": "score",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
}
COLUMNS_ACTIVITIES = {
    "MD_ID": "activity_id",
    "DONOR": "donor_code",
    "RECIPIENT": "recipient_code",
    "SECTOR": "sector_code",
    "MARKER": "marker",
    "SCORE": "score",
    "CLIMATE_MITIGATION": "climate_mitigation_score",
    "CLIMATE_ADAPTATION": "climate_adaptation_score",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
}

# Files with totals, which overlap partially (e.g. DAC members to all developing countries is in all of them).
FILES_TOTALS = ["totals_by_donor.csv", "totals_by_recipient.csv", "totals_by_sector.csv"]

# Codelists with the names of donors and recipients (one codelist for both), and sectors.
CODELISTS = {"CL_AREA_ORG": "area", "CL_DAC_SECTOR": "sector"}
# Hierarchies of groups of recipients and donors.
FILES_GROUPS = {"recipient_groups.xml": "recipient", "donor_groups.xml": "donor"}

# SDMX namespaces.
NS_STRUCTURE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
NS_COMMON = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
NS_XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("climate_related_official_development_assistance.zip")

    # Load data from snapshot.
    tables_totals = [snap.read_in_archive(file_name, low_memory=False) for file_name in FILES_TOTALS]
    tb_activities = snap.read_in_archive("activities.csv", low_memory=False)
    snap_crs = paths.load_snapshot("bilateral_official_development_assistance_commitments.csv")
    tb_bilateral = snap_crs.read(low_memory=False)
    with zipfile.ZipFile(snap.path) as zf:
        structure = ET.fromstring(zf.read("structure.xml"))
        groups = {kind: ET.fromstring(zf.read(file_name)) for file_name, kind in FILES_GROUPS.items()}

    #
    # Process data.
    #
    for file_name, tb in zip(FILES_TOTALS, tables_totals):
        sanity_check_fixed_dimensions(tb=tb, name=file_name, expected=EXPECTED_FIXED_VALUES)
    sanity_check_fixed_dimensions(tb=tb_activities, name="activities.csv", expected=EXPECTED_FIXED_VALUES)
    sanity_check_fixed_dimensions(tb=tb_bilateral, name="CRS totals", expected=EXPECTED_FIXED_VALUES_CRS)

    # Total bilateral ODA by donor and recipient. All DAC members to all developing countries appears in both queries.
    tb_bilateral = tb_bilateral[["DONOR", "RECIPIENT", "TIME_PERIOD", "OBS_VALUE"]].rename(
        columns={"DONOR": "donor_code", "RECIPIENT": "recipient_code", "TIME_PERIOD": "year", "OBS_VALUE": "value"},
        errors="raise",
    )
    tb_bilateral = tb_bilateral.drop_duplicates()
    assert not tb_bilateral.duplicated(subset=["donor_code", "recipient_code", "year"]).any(), (
        "CRS queries disagree on overlapping values."
    )

    # Combine totals, dropping rows that appear in more than one file.
    tb_totals = pr.concat(
        [tb[list(COLUMNS_TOTALS)].rename(columns=COLUMNS_TOTALS, errors="raise") for tb in tables_totals],
        ignore_index=True,
    )
    keys = [column for column in COLUMNS_TOTALS.values() if column != "value"]
    tb_totals = tb_totals.drop_duplicates()
    assert not tb_totals.duplicated(subset=keys).any(), "Files with totals disagree on overlapping values."
    tb_totals = tb_totals.astype({"sector_code": "string"})

    tb_activities = tb_activities[list(COLUMNS_ACTIVITIES)].rename(columns=COLUMNS_ACTIVITIES, errors="raise")
    tb_activities = tb_activities.astype({"sector_code": "string"})

    for tb in [tb_totals, tb_activities]:
        for column in ["donor_code", "recipient_code", "sector_code"]:
            tb[column] = tb[column].astype("category")

    # Names of donors, recipients and sectors, and the hierarchy of sectors.
    tb_codes = snap.read_from_records(extract_codelists(structure=structure))
    assert set(tb_codes["codelist"]) == set(CODELISTS.values()), "Missing codelists in the structure."
    # Hierarchy of groups of recipients and donors (each group with its direct children).
    tb_groups = snap.read_from_records(
        [row for kind, root in groups.items() for row in extract_hierarchy(root=root, kind=kind)]
    ).drop_duplicates()

    # Improve tables format.
    tables = [
        tb_totals.format(keys, short_name="totals"),
        tb_activities.format(["activity_id"], short_name="activities"),
        tb_codes.format(["codelist", "code"], short_name="codes"),
        tb_groups.format(["hierarchy", "parent_code", "child_code"], short_name="groups"),
        tb_bilateral.format(["donor_code", "recipient_code", "year"], short_name="bilateral_totals"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def extract_codelists(structure: ET.Element) -> list[dict]:
    """Extract the English name and parent of every code in the relevant codelists."""
    rows = []
    for codelist in structure.iter(NS_STRUCTURE + "Codelist"):
        if codelist.get("id") not in CODELISTS:
            continue
        for code in codelist.findall(NS_STRUCTURE + "Code"):
            name = next(n.text for n in code.findall(NS_COMMON + "Name") if n.get(NS_XML_LANG) == "en")
            parent = code.find(NS_STRUCTURE + "Parent/Ref")
            rows.append(
                {
                    "codelist": CODELISTS[codelist.get("id")],
                    "code": code.get("id"),
                    "name": name,
                    "parent_code": parent.get("id") if parent is not None else None,
                }
            )
    return rows


def extract_hierarchy(root: ET.Element, kind: str) -> list[dict]:
    """Extract parent-child relations of the main hierarchy of a hierarchical codelist."""
    hierarchy = next(h for h in root.iter(NS_STRUCTURE + "Hierarchy") if h.get("id") == "H")
    rows = []

    def walk(node: ET.Element, parent: str | None) -> None:
        for child in node.findall(NS_STRUCTURE + "HierarchicalCode"):
            code = child.find(NS_STRUCTURE + "CodeID/Ref").get("id")  # ty: ignore
            if parent is not None:
                rows.append({"hierarchy": kind, "parent_code": parent, "child_code": code})
            walk(child, code)

    walk(hierarchy, None)
    return rows


def sanity_check_fixed_dimensions(tb: Table, name: str, expected: dict) -> None:
    """Check that the dimensions fixed by the snapshot queries (including the constant-price base year) are as expected."""
    for column, value in expected.items():
        values = set(tb[column].unique())
        assert values == {value}, (
            f"Unexpected values of {column} in {name}: {sorted(values)}. Expected {value!r}. If the base year changed, "
            "update CONSTANT_PRICE_BASE_YEAR here AND `definitions.inflation_year` in the garden .meta.yml."
        )
