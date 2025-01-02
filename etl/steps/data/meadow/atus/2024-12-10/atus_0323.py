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
    55: "Co-workers/colleagues/clients",  # - old code
    56: "Neighbors/acquaintances",
    57: "Other nonhousehold children < 18",
    58: "Other nonhousehold adults 18 and older",
    59: "Boss or manager",
    60: "People whom I supervise",
    61: "Co-workers",
    62: "Customers",
    "NA": "Unknown",
    -1: "Not applicable",
    -2: "Not applicable",
    -3: "Not applicable",
}

WHO_CODE_CATEGORIES = {
    18: "Alone",
    19: "Alone",  # no distinction between 18 and 19
    20: "Partner",
    21: "Partner",
    22: "Children",
    23: "Children",
    24: "Family",
    25: "Family",
    26: "Family",
    27: "Children",
    28: "Friend",
    29: "Friend",
    30: "Other",
    40: "Children",
    51: "Family",
    52: "Children",
    53: "Family",
    54: "Friend",
    55: "Co-worker",
    56: "Other",
    57: "Other",  # Maybe also children?
    58: "Other",
    59: "Co-worker",
    60: "Co-worker",
    61: "Co-worker",
    62: "Other",  # should be co-worker
    "NA": "Unknown",
    -1: "Not applicable",
    -2: "Not applicable",
    -3: "Not applicable",
}


# TODO: figure out why line number is sometimes negative?


ACTIVITY_COL_DICT = {
    "TUCASEID": "case_id",
    "TUACTIVITY_N": "activity_number",
    "TUACTDUR24": "activity_duration_24",
    "TUCC5": "own_hh_children_present",
    "TUCC5B": "other_hh_children_present",
    "TRTCCTOT_LN": "time_sec_cc_total",
    "TRTCC_LN": "time_sec_cc_hh_and_own_nhh_children",
    "TRTCOC_LN": "time_sec_cc_non_hh_non_own_children",
    "TUSTARTTIM": "activity_start_time",
    "TUSTOPTIME": "activity_end_time",
    "TRCODEP": "activity_code",
    "TRTIER1P": "activity_tier1",
    "TRTIER2P": "activity_tier2",
    "TUCC8": "other_children_present",
    "TUCUMDUR": "cum_activity_duration",
    "TUCUMDUR24": "cum_activity_duration_24",
    "TUACTDUR": "activity_duration",
    "TR_03CC57": "allocation_flag_TR_03CC57",
    # ?
    "TRTO_LN": "time_sec_cc_own_children",
    "TRTONHH_LN": "time_sec_cc_own_nhh_children",
    "TRTOHH_LN": "time_sec_cc_own_hh_children",
    "TRTHH_LN": "time_sec_cc_all_hh_children",
    "TRTNOHH_LN": "time_sec_cc_other_hh_children",
    "TEWHERE": "place_during_activity",
    "TUCC7": "own_nhh_children_present",
    "TRWBELIG": "well_being_eligibility",
    "TRTEC_LN": "time_eldercare",
    "TUEC24": "eldercare",
    "TUDURSTOP": "method_activity_duration",
}

WHO_COL_DICT = {
    "TUCASEID": "case_id",
    "TULINENO": "line_number",
    "TUACTIVITY_N": "activity_number",
    "TRWHONA": "who_na",  # whether respondent was asked who was present
    "TUWHO_CODE": "who_code",  # who was present with respondent
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_who = paths.load_snapshot("atus_who.zip")
    snap_act = paths.load_snapshot("atus_activities.zip")
    # snap_sum = paths.load_snapshot("atus_summary.zip")
    # snap_roster = paths.load_snapshot("atus_roster.zip")
    snap_act_codes = paths.load_snapshot("activity_codes_2023.xls")

    # load tables:
    who_data = load_data_and_add_meta(snap_who, "atuswho_0323.dat")
    act_data = load_data_and_add_meta(snap_act, "atusact_0323.dat")
    act_codes = pr.read_excel(snap_act_codes.path, sheet_name="ATUS 2023 Lexicon", header=1)
    # sum_data = load_data_and_add_meta(snap_sum, "atussum_0323.dat")
    # roster_data = load_data_and_add_meta(snap_roster, "atusrost_0323.dat")

    # format act codes:
    act_codes = act_codes.rename(columns={"6-digit activity code": "activity_code", "Activity": "activity_name"})
    act_codes = act_codes[["activity_code", "activity_name"]]
    act_codes = act_codes.dropna()

    # Rename columns in WHO and ACT tables.
    who_data = who_data.rename(columns=WHO_COL_DICT).reset_index()
    act_data = act_data.rename(columns=ACTIVITY_COL_DICT).reset_index()

    # add column for who was present with respondent
    # error arises from processing log implementation
    try:
        who_data["who_string"] = who_data["who_code"].map(WHO_CODES)
        who_data["who_category"] = who_data["who_code"].map(WHO_CODE_CATEGORIES)

    except AttributeError as e:
        print("processing log is not fully implemented yet")
        print(e)

    # Merge activity and who data:
    tb = act_data.merge(who_data, on=["case_id", "activity_number"], how="left")

    # aggregate by category:
    tb_agg = (
        tb.groupby(["who_category", "case_id", "activity_number"]).agg("activity_duration_24", "first").reset_index()
    )

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
