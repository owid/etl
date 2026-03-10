"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    57: "Other",
    58: "Other",
    59: "Co-worker",
    60: "Co-worker",
    61: "Co-worker",
    62: "Other",  # could be co-worker
    "NA": "Unknown",
    -1: "Not applicable",
    -2: "Not applicable",
    -3: "Not applicable",
}

SUM_COL_DICT = {
    "TUCASEID": "case_id",
    "GEMETSTA": "metropolitan_status",
    "PEEDUCA": "education_level",  # 31-46
    "PEHSPNON": "hispanic",  # 1 hispanic, 2 non-hispanic
    "PTDTRACE": "race",  # topcoded 1-26
    "TEAGE": "age",
    "TELFS": "labor_force_status",
    "TEMJOT": "multiple_jobs",  # 1 yes, 2 no
    "TESCHENR": "school_enrollment",  # 1 yes, 2 no
    "TESCHLVL": "school_level",  # 1 high school, 2 college/ university
    "TESEX": "gender",  # 1 male, 2 female
    "TESPEMPNOT": "employment_status_spouse",  # 1 employed, 2 not employed
    "TRCHILDNUM": "num_hh_children",
    "TRDPFTPT": "full_or_part_time",  # 1 full time, 2 part time
    "TRERNWA": "weekly_earnings",  # implied decimals, topcoded at 288461 (2884.61 USD)
    "TRHOLIDAY": "holiday",  # 0 no, 1 yes
    "TRSPFTPT": "full_or_part_time_spouse",  # 1 full time, 2 part time
    "TRSPPRES": "spouse_present",  # 1 yes, 2 unmarried partner present, 3 no
    "TRYHHCHILD": "youngest_hh_child_age",
    "TUDIARYDAY": "diary_day",  # 1 sunday, 2 monday, ... 7 saturday
    "TUFNWGTP": "final_weight",  # final statistical weight
    "TEHRUSLT": "hours_worked_week",
    "TUYEAR": "year",
    "TU20FWGT": "final_weight_2020",  # 2020 weight
}


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


def run() -> None:
    # Retrieve snapshots.
    snap_who = paths.load_snapshot("atus_who.zip")
    snap_act = paths.load_snapshot("atus_act.zip")
    snap_sum = paths.load_snapshot("atus_sum.zip")

    with snap_who.extracted() as archive:
        tb_who = archive.read("atuswho_0324.dat", force_extension="csv")
    with snap_act.extracted() as archive:
        tb_act = archive.read("atusact_0324.dat", force_extension="csv")
    with snap_sum.extracted() as archive:
        tb_sum = archive.read("atussum_0324.dat", force_extension="csv")

    # Rename columns in WHO and ACT tables.
    tb_who = tb_who.rename(columns=WHO_COL_DICT).reset_index()
    tb_act = tb_act.rename(columns=ACTIVITY_COL_DICT).reset_index()

    # add column for who was present with respondent
    # error arises from processing log implementation

    tb_who["who_string"] = tb_who["who_code"].map(WHO_CODES)
    tb_who["who_category"] = tb_who["who_code"].map(WHO_CODE_CATEGORIES)

    # filter summary data to relevant columns:
    tb_sum = tb_sum.rename(columns=SUM_COL_DICT)
    tb_sum = tb_sum[["case_id", "age", "gender", "year", "final_weight", "final_weight_2020"]]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_who = tb_who.reset_index().format(["index"], short_name="atus_who")
    tb_act = tb_act.reset_index().format(["index"], short_name="atus_act")
    tb_sum = tb_sum.format(["case_id"], short_name="atus_sum")

    # drop level_0 (is index)
    tb_who = tb_who.drop(columns=["level_0"])
    tb_act = tb_act.drop(columns=["level_0"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(
        tables=[tb_who, tb_act, tb_sum], check_variables_metadata=True, default_metadata=snap_who.metadata, repack=False
    )

    # Save meadow dataset.
    ds_meadow.save()
