"""Load a snapshot and create a meadow dataset."""

from xml.etree import ElementTree

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from owid.datautils import dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# XML namespaces used in the SDMX responses.
SDMX_GENERIC = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}"
SDMX_STRUCTURE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
SDMX_COMMON = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"

# Codelists in the structure file that map dimension codes to human-readable names.
CODELISTS = {
    "REF_AREA": "CL_AREA",
    "COMMODITY": "CL_COMMODITY_ENERGY_BALANCE_UNDATA",
    "TRANSACTION": "CL_TRANS_ENERGY_BALANCE_UNDATA",
    "UNIT": "CL_UNIT_ENERGY_UNDATA",
}


def parse_codelists(structure_xml: bytes) -> dict[str, dict[str, str]]:
    """Parse the codelists from the SDMX structure XML into {dimension: {code: name}} mappings."""
    root = ElementTree.fromstring(structure_xml)
    codelists = {cl.attrib["id"]: cl for cl in root.iter(f"{SDMX_STRUCTURE}Codelist")}
    mappings = {}
    for dimension, codelist_id in CODELISTS.items():
        codes = codelists[codelist_id].findall(f"{SDMX_STRUCTURE}Code")
        # NOTE: Names are not unique across all codes of a codelist (e.g. CL_AREA contains deprecated duplicates), but
        # they must be unique among the codes present in the data, which the final format() integrity check enforces.
        mappings[dimension] = {code.attrib["id"]: code.find(f"{SDMX_COMMON}Name").text for code in codes}  # type: ignore[union-attr]
    return mappings


def parse_data(data_xml_path) -> pd.DataFrame:
    """Parse the SDMX-ML GenericData XML into a dataframe of coded observations.

    Each series carries the key (REF_AREA, COMMODITY, TRANSACTION, UNIT), and each observation within it carries the
    year, the value, and attributes (ESTIMATE flag and UNIT_MULT multiplier).
    """
    dimensions = ["REF_AREA", "COMMODITY", "TRANSACTION", "UNIT"]
    columns = {name: [] for name in dimensions + ["year", "value", "ESTIMATE", "UNIT_MULT"]}
    for event, element in ElementTree.iterparse(data_xml_path, events=("end",)):
        if element.tag != f"{SDMX_GENERIC}Series":
            continue
        series_key = {value.attrib["id"]: value.attrib["value"] for value in element.find(f"{SDMX_GENERIC}SeriesKey")}
        for observation in element.findall(f"{SDMX_GENERIC}Obs"):
            year = None
            value = None
            attributes = {}
            for child in observation:
                if child.tag == f"{SDMX_GENERIC}ObsDimension":
                    year = child.attrib["value"]
                elif child.tag == f"{SDMX_GENERIC}ObsValue":
                    value = child.attrib["value"]
                elif child.tag == f"{SDMX_GENERIC}Attributes":
                    attributes = {v.attrib["id"]: v.attrib["value"] for v in child}
            for dimension in dimensions:
                columns[dimension].append(series_key[dimension])
            columns["year"].append(year)
            columns["value"].append(value)
            columns["ESTIMATE"].append(attributes.get("ESTIMATE"))
            columns["UNIT_MULT"].append(attributes.get("UNIT_MULT"))
        # Free memory as we go (the full file is ~270 MB of XML).
        element.clear()
    return pd.DataFrame(columns)


def read_snapshot(snap) -> tuple[Table, dict[str, dict[str, str]]]:
    """Read the data and codelists from the snapshot zip archive."""
    with snap.extracted() as archive:
        with open(archive.path / "structure.xml", "rb") as f:
            codelists = parse_codelists(f.read())
        df = parse_data(archive.path / "energy_balances.xml")
    tb = pr.read_df(df, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)
    return tb, codelists


def sanity_check_inputs(tb: Table, codelists: dict[str, dict[str, str]]) -> None:
    """Check raw data before processing."""
    assert len(tb) > 1e6, f"Expected at least a million rows, got {len(tb)}"
    for dimension in ["REF_AREA", "COMMODITY", "TRANSACTION", "UNIT"]:
        unmapped = set(tb[dimension]) - set(codelists[dimension])
        assert not unmapped, f"Unmapped {dimension} codes: {unmapped}"
    assert tb["value"].notna().all(), "Unexpected missing observation values"
    # All values in this dataflow are expressed in plain units (multiplier 10^0); if this ever changes, values would
    # need to be rescaled by 10^UNIT_MULT.
    assert set(tb["UNIT_MULT"]) == {"0"}, f"Unexpected unit multipliers: {set(tb['UNIT_MULT'])}"


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("energy_balances.zip")
    tb, codelists = read_snapshot(snap)

    # Sanity check inputs.
    sanity_check_inputs(tb, codelists)

    #
    # Process data.
    #
    # Map coded dimensions to human-readable names using codelists.
    # NOTE: warn_on_unused_mappings is left off because codelists legitimately contain many codes absent from the data.
    tb["country"] = dataframes.map_series(tb["REF_AREA"], mapping=codelists["REF_AREA"], warn_on_missing_mappings=True)
    tb["commodity"] = dataframes.map_series(
        tb["COMMODITY"], mapping=codelists["COMMODITY"], warn_on_missing_mappings=True
    )
    tb["transaction"] = dataframes.map_series(
        tb["TRANSACTION"], mapping=codelists["TRANSACTION"], warn_on_missing_mappings=True
    )
    tb["unit"] = dataframes.map_series(tb["UNIT"], mapping=codelists["UNIT"], warn_on_missing_mappings=True)
    tb["estimate"] = tb["ESTIMATE"]

    # Parse types.
    tb["year"] = pr.to_numeric(tb["year"], downcast="integer")
    tb["value"] = pr.to_numeric(tb["value"])

    # Keep only the resolved columns and the value.
    tb = tb[["country", "year", "commodity", "transaction", "unit", "estimate", "value"]].copy()

    # Set index and sort.
    tb = tb.format(["country", "year", "commodity", "transaction", "unit"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
