"""Load a snapshot and create a meadow dataset."""

from zipfile import ZipFile

from owid.catalog import Table, TableMeta
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Codes for who was present with respondent.
# see also: https://www.bls.gov/tus/dictionaries/atusintcodebk23.pdf
WHO_CODES = {
    18: "Alone",
    19: "Alone",  # no distinction between 18 and 19
    20: "Spouse",
    21: "Unmarried partner",
    22: "Own household child",
    23: "Grandchild",
    24: "Parent",
    25: "Brother/sister",
    26: "Other related person",
    27: "Foster child",
    28: "Housemate/roommate",
    29: "Roomer/boarder",
    30: "Other nonrelative",
    40: "Own nonhousehold child < 18",
    51: "Parents (not living in household)",
    52: "Other nonhousehold family members < 18",
    53: "Other nonhousehold family members 18 and older (including parents-in-law)",
    54: "Friends",
    # 55: "Co-workers/colleagues/clients", - old code
    56: "Neighbors/acquaintances",
    57: "Other nonhousehold children < 18",
    58: "Other nonhousehold adults 18 and older",
    59: "Boss or manager",
    60: "People whom I supervise",
    61: "Co-workers",
    62: "Customers",
    "NA": "Unknown",
    -1: "Not applicable",
}

# TODO: figure out why line number is sometimes negative?


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_who = paths.load_snapshot("atus_who.zip")
    snap_act = paths.load_snapshot("atus_activities.zip")
    snap_roster = paths.load_snapshot("atus_roster.zip")
    snap_act_codes = paths.load_snapshot("atus_activity_codes.zip")

    # load tables:
    who_data = load_data_and_add_meta(snap_who, "atuswho_0323.dat")
    act_data = load_data_and_add_meta(snap_act, "atusact_0323.dat")
    roster_data = load_data_and_add_meta(snap_roster, "atusrost_0323.dat")

    # Rename columns in WHO file
    who_data = who_data.rename(
        columns={
            "TUCASEID": "case_id",
            "TULINENO": "line_number",
            "TUACTIVITY_N": "activity_number",
            "TRWHONA": "who_na",  # whether respondent was asked who was present
            "TUWHO_CODE": "who_code",  # who was present with respondent
        }
    ).reset_index()

    # add column for who was present with respondent
    who_data["who_string"] = who_data["who_code"].replace(WHO_CODES)

    for col in who_data.columns:
        who_data[col].metadata = tb_meta
        who_data[col].metadata.origins = [snap_who.metadata.origin]
    who_data.metadata = tb_meta

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = who_data.format(["index"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_who.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def load_data_and_add_meta(snap, file_name):
    zf = ZipFile(snap.path)
    tb = pr.read_csv(zf.open(file_name), origin=snap.metadata.origin)

    tb_meta = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.origin.title,
        description=snap.metadata.origin.description,
    )
    for col in tb.columns:
        tb[col].metadata = tb_meta
        tb[col].metadata.origins = [snap.metadata.origin]
    tb.metadata = tb_meta

    return tb
